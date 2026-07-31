//! Compat persistence: read/write real SQLite-format database files.
//!
//! Bridges the in-memory `MemDatabase` to on-disk SQLite files via the
//! pager + B-tree stack. The VDBE continues to execute against `MemDatabase`;
//! this module serializes/deserializes that state to proper binary format.
//!
//! On **persist**, all tables and their rows are written to a real SQLite
//! database file (with a valid header, sqlite_master, and B-tree pages).
//!
//! On **load**, a real `.db` file is read via B-tree cursors and its
//! contents are replayed into a fresh `MemDatabase` + schema vector.

#![cfg_attr(
    any(target_arch = "wasm32", not(feature = "native")),
    allow(dead_code, unused_imports)
)]

#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
use std::collections::{HashMap, HashSet};
#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
use std::hash::BuildHasher;
#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
use std::path::Path;

use fsqlite_ast::{
    ColumnConstraintKind, ConflictAction, CreateTableBody, CreateTableStatement, DefaultValue,
    Expr, GeneratedStorage, IndexedColumn, Literal, SortDirection, Statement, TableConstraintKind,
    UnaryOp,
};
#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
use fsqlite_btree::BtreeCursorOps;
#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
use fsqlite_btree::cursor::TransactionPageIo;
use fsqlite_error::{FrankenError, Result};
#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
use fsqlite_pager::{MvccPager, SimplePager, TransactionHandle, TransactionMode};
use fsqlite_parser::Parser;
use fsqlite_types::StrictColumnType;
#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
use fsqlite_types::cx::Cx;
#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
use fsqlite_types::record::{
    RecordProfileScope, enter_record_profile_scope, parse_record, serialize_record,
};
use fsqlite_types::value::SqliteValue;

#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
use crate::connection::{
    eval_join_expr, is_sqlite_truthy, validate_sqlite_schema_catalog_from_master_entries,
};
use fsqlite_types::{DATABASE_HEADER_SIZE, DatabaseHeader, PageNumber, PageSize};
use fsqlite_vdbe::codegen::{
    CheckConstraint, ColumnInfo, FkActionType, FkDef, IndexSchema, PrimaryKeyConstraint,
    TableSchema,
};
use fsqlite_vdbe::engine::MemDatabase;
#[cfg(all(not(target_arch = "wasm32"), feature = "native", unix))]
use fsqlite_vfs::UnixVfs as PlatformVfs;
#[cfg(all(not(target_arch = "wasm32"), feature = "native", target_os = "windows"))]
use fsqlite_vfs::WindowsVfs as PlatformVfs;
#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
use fsqlite_vfs::{FileIdentity, host_fs};

/// SQLite file header magic bytes (first 16 bytes).
#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
const SQLITE_MAGIC: &[u8; 16] = b"SQLite format 3\0";

/// Default page size used for newly-created databases.
#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
const DEFAULT_PAGE_SIZE: PageSize = PageSize::DEFAULT;

/// Owned sqlite_master row payload used when persistence must preserve
/// non-table entries such as views and triggers during file rebuilds.
pub type SqliteMasterEntry = (String, String, String, u32, Option<String>);

/// Select the SQL text persisted for an index entry in `sqlite_master`.
///
/// A stored, non-NULL SQL definition is authoritative: it identifies an
/// explicit index even when a legacy database gave that index a reserved
/// `sqlite_autoindex_*`-looking name. Only an index without preserved DDL whose
/// name canonically maps to its table and a positive decimal ordinal may be
/// classified as an implicit autoindex and serialized with NULL SQL.
#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
fn index_sql_for_persistence<S, F>(
    index_name: &str,
    table_name: &str,
    original_ddl: &HashMap<String, String, S>,
    synthesize: F,
) -> Option<String>
where
    S: BuildHasher,
    F: FnOnce() -> String,
{
    if let Some(original) = original_ddl.get(&index_name.to_ascii_lowercase()) {
        return Some(original.clone());
    }
    if parse_autoindex_ordinal(index_name, table_name).is_some() {
        return None;
    }
    Some(synthesize())
}

fn parse_autoindex_ordinal(index_name: &str, table_name: &str) -> Option<usize> {
    let index_name_lower = index_name.to_ascii_lowercase();
    let prefix = format!("sqlite_autoindex_{}_", table_name.to_ascii_lowercase());
    let suffix = index_name_lower.strip_prefix(&prefix)?;
    if suffix.is_empty() || suffix.starts_with('0') || !suffix.chars().all(|ch| ch.is_ascii_digit())
    {
        return None;
    }
    let ordinal = suffix.parse::<usize>().ok()?;
    (ordinal.to_string() == suffix).then_some(ordinal)
}

#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
fn load_sqlite_cursor_sizes_from_page1(page1_bytes: &[u8]) -> Result<(u32, u32)> {
    let header_bytes: &[u8; DATABASE_HEADER_SIZE] = page1_bytes
        .get(..DATABASE_HEADER_SIZE)
        .ok_or_else(|| FrankenError::DatabaseCorrupt {
            detail: format!(
                "database header truncated: expected at least {DATABASE_HEADER_SIZE} bytes, found {}",
                page1_bytes.len()
            ),
        })?
        .try_into()
        .map_err(|_| FrankenError::DatabaseCorrupt {
            detail: "database header is not a fixed-size 100-byte prefix".to_owned(),
        })?;
    let header = DatabaseHeader::from_bytes(header_bytes).map_err(|error| {
        FrankenError::DatabaseCorrupt {
            detail: format!("invalid database header: {error}"),
        }
    })?;
    Ok((
        header.page_size.usable(header.reserved_per_page),
        header.page_size.get(),
    ))
}

#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
fn configure_btree_cursor_page_size<P: fsqlite_btree::PageReader>(
    cursor: &mut fsqlite_btree::BtCursor<P>,
    usable_size: u32,
    page_size: u32,
) {
    if page_size != usable_size {
        cursor.set_page_size(page_size);
    }
}

// ── Public API ──────────────────────────────────────────────────────────

/// State loaded from a real SQLite file.
#[derive(Debug)]
pub struct LoadedState {
    /// Reconstructed table schemas.
    pub schema: Vec<TableSchema>,
    /// In-memory database populated with all rows.
    pub db: MemDatabase,
    /// Number of sqlite_master entries loaded (the next available rowid
    /// for sqlite_master is `master_row_count + 1`).
    pub master_row_count: i64,
    /// Schema cookie read from the database header (offset 40).
    pub schema_cookie: u32,
    /// File change counter read from the database header (offset 24).
    pub change_counter: u32,
}

/// Detect whether a file starts with the SQLite magic header.
///
/// Returns `false` for non-existent, empty, or non-SQLite files.
#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
pub fn is_sqlite_format(path: &Path) -> bool {
    let Ok(data) = host_fs::read(path) else {
        return false;
    };
    data.len() >= SQLITE_MAGIC.len() && data[..SQLITE_MAGIC.len()] == *SQLITE_MAGIC
}

/// Persist `schema` + `db` to a real SQLite-format database file at `path`.
///
/// Overwrites any existing file. The resulting file is readable by `sqlite3`.
/// The caller supplies the capability context so pager and B-tree work stay
/// attached to the active runtime lineage.
///
/// # Errors
///
/// Returns an error on I/O failure or if the B-tree layer rejects an
/// insertion (e.g. duplicate rowid in sqlite_master).
#[allow(clippy::too_many_lines)]
#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
pub async fn persist_to_sqlite(
    cx: &Cx,
    path: &Path,
    schema: &[TableSchema],
    db: &MemDatabase,
    schema_cookie: u32,
    change_counter: u32,
) -> Result<()> {
    let mut header = DatabaseHeader {
        page_size: DEFAULT_PAGE_SIZE,
        schema_cookie,
        change_counter,
        ..DatabaseHeader::default()
    };
    let effective_counter = header.change_counter.max(1);
    header.change_counter = effective_counter;
    header.schema_cookie = header.schema_cookie.max(1);
    header.version_valid_for = effective_counter;
    persist_to_sqlite_with_header(cx, path, schema, db, &header).await
}

/// Persist `schema` + `db` using the provided database header template.
///
/// The supplied `header` controls page-size-sensitive layout plus header
/// metadata that must survive rebuild flows like `VACUUM`.
#[allow(clippy::too_many_lines)]
#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
pub async fn persist_to_sqlite_with_header(
    cx: &Cx,
    path: &Path,
    schema: &[TableSchema],
    db: &MemDatabase,
    header_template: &DatabaseHeader,
) -> Result<()> {
    persist_to_sqlite_with_header_and_master_entries(
        cx,
        path,
        schema,
        db,
        header_template,
        &[],
        &HashMap::new(),
    )
    .await
}

/// Persist `schema` + `db` plus additional sqlite_master rows using the
/// provided database header template.
#[allow(clippy::too_many_lines)]
#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
pub async fn persist_to_sqlite_with_header_and_master_entries<S: BuildHasher>(
    cx: &Cx,
    path: &Path,
    schema: &[TableSchema],
    db: &MemDatabase,
    header_template: &DatabaseHeader,
    extra_master_entries: &[SqliteMasterEntry],
    original_ddl: &HashMap<String, String, S>,
) -> Result<()> {
    persist_to_sqlite_with_header_and_master_entries_impl(
        cx,
        path,
        schema,
        db,
        header_template,
        extra_master_entries,
        original_ddl,
        None,
    )
    .await
}

/// Persist into an atomically caller-reserved empty file.
///
/// The path is opened only through the pager's identity-bound `ReservedEmpty`
/// mode. A missing, replaced, non-empty, or sidecar-bearing reservation is
/// rejected before any database byte is initialized.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
pub async fn persist_to_reserved_sqlite_with_header_and_master_entries<S: BuildHasher>(
    cx: &Cx,
    path: &Path,
    expected_identity: FileIdentity,
    schema: &[TableSchema],
    db: &MemDatabase,
    header_template: &DatabaseHeader,
    extra_master_entries: &[SqliteMasterEntry],
    original_ddl: &HashMap<String, String, S>,
) -> Result<()> {
    persist_to_sqlite_with_header_and_master_entries_impl(
        cx,
        path,
        schema,
        db,
        header_template,
        extra_master_entries,
        original_ddl,
        Some(expected_identity),
    )
    .await
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
async fn persist_to_sqlite_with_header_and_master_entries_impl<S: BuildHasher>(
    cx: &Cx,
    path: &Path,
    schema: &[TableSchema],
    db: &MemDatabase,
    header_template: &DatabaseHeader,
    extra_master_entries: &[SqliteMasterEntry],
    original_ddl: &HashMap<String, String, S>,
    expected_empty_identity: Option<FileIdentity>,
) -> Result<()> {
    // Reject unsupported or internally inconsistent WITHOUT ROWID shapes
    // before replacing/opening the target. A failed export must not leave a
    // partially initialized SQLite file behind.
    let prepared_without_rowid = prepare_without_rowid_persistence(schema, db, original_ddl)?;

    if expected_empty_identity.is_none() && path.exists() {
        // Legacy overwrite callers deliberately replace their own target.
        // Identity-sensitive exports must use the reserved entry point above.
        host_fs::create_empty_file(path)?;
    }

    let vfs = PlatformVfs::new();
    let pager = if let Some(expected_identity) = expected_empty_identity {
        SimplePager::open_reserved_with_cx_and_page_buffer_max(
            cx,
            vfs,
            path,
            header_template.page_size,
            expected_identity,
            None,
        )
        .await?
    } else {
        SimplePager::open_with_cx(cx, vfs, path, header_template.page_size).await?
    };
    let mut txn = pager.begin(cx, TransactionMode::Immediate).await?;

    let page_size = header_template.page_size;
    let page_size_usize = page_size.as_usize();
    let usable_size = page_size.usable(header_template.reserved_per_page);
    let full_page_size = page_size.get();

    // Track (type, name, tbl_name, root_page, create_sql) for sqlite_master entries.
    // Extended from just tables to also include indexes, views, and triggers.
    // The sql column is Option<String> because autoindex entries (sqlite_autoindex_*)
    // must have NULL sql, matching stock SQLite behavior.
    let mut master_entries: Vec<SqliteMasterEntry> = Vec::new();

    // Write each table's data into its own B-tree.
    for table in schema {
        let Some(mem_table) = db.get_table(table.root_page) else {
            continue;
        };
        let without_rowid_plan = prepared_without_rowid.get(&table.name.to_ascii_lowercase());

        // Allocate a fresh root page for this table in the on-disk file.
        let root_page = txn.allocate_page(cx).await?;

        // WITHOUT ROWID tables use an index B-tree rooted at a leaf-index
        // page. Ordinary tables use the rowid table B-tree format.
        if without_rowid_plan.is_some() {
            init_leaf_index_page(cx, &mut txn, root_page, page_size_usize, usable_size).await?;
        } else {
            init_leaf_table_page(cx, &mut txn, root_page, page_size_usize, usable_size).await?;
        }

        // Insert all rows.
        if let Some(without_rowid_plan) = without_rowid_plan {
            let table_layout = &without_rowid_plan.table_layout;
            let mut cursor = fsqlite_btree::BtCursor::new_with_index_desc(
                TransactionPageIo::new(&mut txn),
                root_page,
                usable_size,
                false,
                table_layout.table_cursor_desc_flags(),
            );
            let collation_registry = cursor.collation_registry();
            cursor.set_index_collation_context(
                table_layout.table_cursor_collations(),
                collation_registry,
            );
            configure_btree_cursor_page_size(&mut cursor, usable_size, full_page_size);
            let primary_key_label = format!(
                "{}.{}",
                table.name,
                table_layout
                    .primary_key_declared
                    .iter()
                    .map(|&declared_idx| table.columns[declared_idx].name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            for (_, values) in mem_table.iter_rows() {
                let storage_values =
                    table_layout.storage_values_from_declared(&table.name, values)?;
                let key = serialize_record(&storage_values);
                cursor
                    .index_insert_unique(
                        cx,
                        &key,
                        table_layout.primary_key_len(),
                        &primary_key_label,
                    )
                    .await?;
            }
        } else {
            let mut cursor = fsqlite_btree::BtCursor::new(
                TransactionPageIo::new(&mut txn),
                root_page,
                usable_size,
                true,
            );
            configure_btree_cursor_page_size(&mut cursor, usable_size, full_page_size);
            for (rowid, values) in mem_table.iter_rows() {
                let payload = serialize_record(values);
                cursor.table_insert(cx, rowid, &payload).await?;
            }
        }

        // Prefer the original DDL when available — it preserves column-level
        // CHECK constraints, exact DEFAULT formatting, and constraint ordering
        // that build_create_table_sql might not reconstruct perfectly.
        // Keys in original_ddl are lowercased (per reload_memdb_from_txn_with_mode).
        let create_sql = original_ddl
            .get(&table.name.to_ascii_lowercase())
            .cloned()
            .unwrap_or_else(|| {
                build_create_table_sql_with_implicit_index_predicate(table, |index| {
                    parse_autoindex_ordinal(&index.name, &table.name).is_some()
                        && !original_ddl.contains_key(&index.name.to_ascii_lowercase())
                })
            });
        let table_name = table.name.clone();
        let implicit_constraint_plan = match parse_single_statement(&create_sql) {
            Some(Statement::CreateTable(create)) => {
                plan_implicit_constraint_indexes(&create, &table_name)?
            }
            _ if is_virtual_table_sql(&create_sql) => Vec::new(),
            _ => {
                return Err(FrankenError::Internal(format!(
                    "table `{table_name}` CREATE statement cannot be parsed for implicit constraint-index validation before persistence"
                )));
            }
        };
        let actual_implicit_index_names = table
            .indexes
            .iter()
            .filter(|index| {
                parse_autoindex_ordinal(&index.name, &table_name).is_some()
                    && !original_ddl.contains_key(&index.name.to_ascii_lowercase())
            })
            .map(|index| index.name.clone())
            .collect::<Vec<_>>();
        validate_auxiliary_implicit_index_inventory(
            &table_name,
            &implicit_constraint_plan,
            &actual_implicit_index_names,
        )?;
        master_entries.push((
            "table".to_owned(),
            table_name.clone(),
            table_name.clone(),
            root_page.get(),
            Some(create_sql),
        ));

        // Build column map once for evaluating partial index WHERE predicates.
        // [(table_name, column_name, is_rowid_alias), ...]
        let col_map: Vec<(String, String, bool)> = table
            .columns
            .iter()
            .map(|c| (table.name.clone(), c.name.clone(), false))
            .collect();

        // Write index B-trees for all indexes including autoindexes.
        // Autoindexes (sqlite_autoindex_*) are created for UNIQUE constraints
        // and non-IPK PRIMARY KEY columns. Their sqlite_master entries point to
        // root pages that must contain valid B-tree data. Skipping them causes
        // "wrong # of entries in index" and "page N: never used" errors when
        // stock SQLite runs integrity_check (issue #55).
        for (index_position, index) in table.indexes.iter().enumerate() {
            let is_expression_index = index.columns.is_empty() && !index.key_expressions.is_empty();
            if index.columns.is_empty() && !is_expression_index {
                continue;
            }
            let key_exprs = if is_expression_index {
                index
                    .key_expressions
                    .iter()
                    .map(|expr| {
                        fsqlite_parser::expr::parse_expr(expr).map_err(|err| {
                            FrankenError::Internal(format!(
                                "failed to parse expression index term `{expr}` while persisting `{}`: {err}",
                                index.name
                            ))
                        })
                    })
                    .collect::<Result<Vec<_>>>()?
            } else {
                Vec::new()
            };
            // Allocate and initialize root page as leaf index page (0x0A).
            let idx_root = txn.allocate_page(cx).await?;
            init_leaf_index_page(cx, &mut txn, idx_root, page_size_usize, usable_size).await?;

            // Parse the partial index WHERE clause (if any) so we can skip
            // rows that don't satisfy the predicate.
            let partial_predicate = if without_rowid_plan.is_some() {
                index
                    .where_clause
                    .as_deref()
                    .map(|predicate| {
                        fsqlite_parser::expr::parse_expr(predicate).map_err(|error| {
                            FrankenError::FunctionError(format!(
                                "cannot persist WITHOUT ROWID table `{table_name}` partial index `{}` because its predicate cannot be parsed: {error}",
                                index.name
                            ))
                        })
                    })
                    .transpose()?
            } else {
                index
                    .where_clause
                    .as_deref()
                    .map(fsqlite_parser::expr::parse_expr)
                    .transpose()
                    .ok()
                    .flatten()
            };

            // Populate the index B-tree from table rows.
            {
                let without_rowid_index_layout =
                    without_rowid_plan.map(|plan| &plan.secondary_layouts[index_position]);
                let mut idx_cursor =
                    if let Some(without_rowid_index_layout) = without_rowid_index_layout {
                        let mut cursor = fsqlite_btree::BtCursor::new_with_index_desc(
                            TransactionPageIo::new(&mut txn),
                            idx_root,
                            usable_size,
                            false,
                            without_rowid_index_layout.cursor_desc_flags.clone(),
                        );
                        let collation_registry = cursor.collation_registry();
                        cursor.set_index_collation_context(
                            without_rowid_index_layout.cursor_collations.clone(),
                            collation_registry,
                        );
                        cursor
                    } else {
                        fsqlite_btree::BtCursor::new(
                            TransactionPageIo::new(&mut txn),
                            idx_root,
                            usable_size,
                            true,
                        )
                    };
                configure_btree_cursor_page_size(&mut idx_cursor, usable_size, full_page_size);
                let unique_key_label = (without_rowid_index_layout.is_some() && index.is_unique)
                    .then(|| format!("{}.{}", table.name, index.key_label()));
                if let Some(mem_table) = db.get_table(table.root_page) {
                    for (rowid, values) in mem_table.iter_rows() {
                        // For partial indexes, skip rows that don't match
                        // the WHERE predicate.
                        if let Some(ref predicate) = partial_predicate {
                            if without_rowid_plan.is_some() {
                                let result = eval_join_expr(predicate, values, &col_map)?;
                                if !is_sqlite_truthy(&result) {
                                    continue;
                                }
                            } else if let Ok(result) = eval_join_expr(predicate, values, &col_map)
                                && !is_sqlite_truthy(&result)
                            {
                                continue;
                            }
                            // The legacy rowid path includes the row if
                            // predicate evaluation fails. WITHOUT ROWID
                            // evaluation is fail-closed above and in preflight.
                        }

                        // Rowid-table indexes append the rowid. WITHOUT ROWID
                        // indexes append only those PRIMARY KEY locator columns
                        // not already represented by an equivalent
                        // (same-column, same-collation) logical key term.
                        let mut key_values: Vec<SqliteValue> = Vec::new();
                        if let Some(without_rowid_index_layout) = without_rowid_index_layout {
                            for &declared_idx in &without_rowid_index_layout.logical_declared {
                                key_values.push(
                                    values
                                        .get(declared_idx)
                                        .cloned()
                                        .unwrap_or(SqliteValue::Null),
                                );
                            }
                            for &declared_idx in &without_rowid_index_layout.locator_declared {
                                let value = values.get(declared_idx).ok_or_else(|| {
                                    FrankenError::DatabaseCorrupt {
                                        detail: format!(
                                            "WITHOUT ROWID table `{table_name}` row omits index `{}` PRIMARY KEY locator column `{}`",
                                            index.name, table.columns[declared_idx].name
                                        ),
                                    }
                                })?;
                                key_values.push(value.clone());
                            }
                        } else if is_expression_index {
                            for expr in &key_exprs {
                                key_values.push(eval_join_expr(expr, values, &col_map)?);
                            }
                        } else {
                            for col_name in &index.columns {
                                let col_idx = table
                                    .columns
                                    .iter()
                                    .position(|c| c.name.eq_ignore_ascii_case(col_name));
                                if let Some(idx) = col_idx {
                                    key_values.push(
                                        values.get(idx).cloned().unwrap_or(SqliteValue::Null),
                                    );
                                } else {
                                    key_values.push(SqliteValue::Null);
                                }
                            }
                        }
                        if without_rowid_index_layout.is_none() {
                            key_values.push(SqliteValue::Integer(rowid));
                        }
                        let key = serialize_record(&key_values);
                        if without_rowid_index_layout.is_some() && index.is_unique {
                            idx_cursor
                                .index_insert_unique(
                                    cx,
                                    &key,
                                    index.key_term_count(),
                                    unique_key_label.as_deref().ok_or_else(|| {
                                        FrankenError::Internal(format!(
                                            "WITHOUT ROWID unique index `{}` lost its constraint label",
                                            index.name
                                        ))
                                    })?,
                                )
                                .await?;
                        } else {
                            idx_cursor.index_insert(cx, &key).await?;
                        }
                    }
                }
            }

            // Preserve stored non-NULL DDL before considering the reserved
            // prefix. Legacy databases can contain explicit indexes with a
            // sqlite_autoindex_* name; erasing their SQL turns them into
            // unreconstructable implicit entries. Only names that canonically
            // map to this table and a positive ordinal, with no preserved DDL,
            // are genuine implicit autoindexes.
            let idx_sql = index_sql_for_persistence(&index.name, &table_name, original_ddl, || {
                if is_expression_index {
                    build_create_expression_index_sql(
                        &index.name,
                        &table_name,
                        index.is_unique,
                        &index.key_expressions,
                        &index.key_collations,
                        &index.key_sort_directions,
                        index.where_clause.as_deref(),
                    )
                } else {
                    let terms: Vec<CreateIndexSqlTerm<'_>> = index
                        .columns
                        .iter()
                        .enumerate()
                        .map(|(i, col)| CreateIndexSqlTerm {
                            column_name: col.as_str(),
                            collation: index.key_collations.get(i).and_then(|c| c.as_deref()),
                            direction: index.key_sort_directions.get(i).copied(),
                        })
                        .collect();
                    let sql = build_create_index_sql(
                        &index.name,
                        &table_name,
                        index.is_unique,
                        &terms,
                        None,
                    );
                    if let Some(ref wc) = index.where_clause {
                        format!("{sql} WHERE {wc}")
                    } else {
                        sql
                    }
                }
            });
            master_entries.push((
                "index".to_owned(),
                index.name.clone(),
                table_name.clone(),
                idx_root.get(),
                idx_sql,
            ));
        }
    }

    master_entries.extend(extra_master_entries.iter().cloned());

    // Write sqlite_master entries into page 1's B-tree.
    // sqlite_master columns: type TEXT, name TEXT, tbl_name TEXT, rootpage INTEGER, sql TEXT
    {
        let mut page1 = txn.get_page(cx, PageNumber::ONE).await?.into_vec();
        if page1.len() < DATABASE_HEADER_SIZE + 8 {
            return Err(FrankenError::internal(format!(
                "page 1 too short for sqlite_master root header: {} bytes",
                page1.len()
            )));
        }
        page1[DATABASE_HEADER_SIZE] = 0x0D;
        page1[DATABASE_HEADER_SIZE + 3..DATABASE_HEADER_SIZE + 5]
            .copy_from_slice(&0u16.to_be_bytes());
        let master_content_start: u16 = if usable_size == 65536 {
            0
        } else {
            u16::try_from(usable_size).map_err(|_| {
                FrankenError::internal(format!(
                    "usable_size {usable_size} does not fit in u16 and is not 65536"
                ))
            })?
        };
        page1[DATABASE_HEADER_SIZE + 5..DATABASE_HEADER_SIZE + 7]
            .copy_from_slice(&master_content_start.to_be_bytes());
        txn.write_page(cx, PageNumber::ONE, &page1).await?;

        let master_root = PageNumber::ONE;
        let mut cursor = fsqlite_btree::BtCursor::new(
            TransactionPageIo::new(&mut txn),
            master_root,
            usable_size,
            true,
        );
        configure_btree_cursor_page_size(&mut cursor, usable_size, full_page_size);

        for (rowid, (entry_type, name, tbl_name, root_page_num, create_sql)) in
            master_entries.iter().enumerate()
        {
            let sql_value = match create_sql {
                Some(sql) => SqliteValue::Text(sql.clone().into()),
                None => SqliteValue::Null,
            };
            let record = serialize_record(&[
                SqliteValue::Text(entry_type.clone().into()),
                SqliteValue::Text(name.clone().into()),
                SqliteValue::Text(tbl_name.clone().into()),
                SqliteValue::Integer(i64::from(*root_page_num)),
                sql_value,
            ]);
            #[allow(clippy::cast_possible_wrap)]
            let rid = (rowid as i64) + 1;
            cursor.table_insert(cx, rid, &record).await?;
        }
    }

    // Fix up the database header on page 1: update page_count,
    // change_counter, and schema_cookie so sqlite3 validates the file.
    {
        let mut hdr_page = txn.get_page(cx, PageNumber::ONE).await?.into_vec();

        // Discover the current page count by allocating one more page.
        // The extra page is included in the commit (the pager does not
        // support free_page), so the exported file has one trailing empty
        // page. This is benign: SQLite tolerates pages beyond the last
        // B-tree node, and the page_count header excludes it.
        let next_page = txn.allocate_page(cx).await?.get();
        let max_page = next_page.saturating_sub(1).max(1);

        let mut final_header = header_template.clone();
        final_header.page_count = max_page;
        final_header.freelist_trunk = 0;
        final_header.freelist_count = 0;
        final_header.change_counter = final_header.change_counter.max(1);
        final_header.schema_cookie = final_header.schema_cookie.max(1);
        final_header.version_valid_for = final_header.change_counter;

        let encoded_header = final_header.to_bytes().map_err(|err| {
            FrankenError::internal(format!("failed to encode database header: {err}"))
        })?;
        hdr_page[..DATABASE_HEADER_SIZE].copy_from_slice(&encoded_header);

        txn.write_page(cx, PageNumber::ONE, &hdr_page).await?;
    }

    txn.commit(cx).await?;
    Ok(())
}

/// Load a real SQLite-format database file into `MemDatabase` + schema.
///
/// Reads sqlite_master from page 1, then reads each table's B-tree to
/// populate the in-memory store.
/// The caller supplies the capability context so pager reads inherit the
/// active trace and budget lineage.
///
/// # Errors
///
/// Returns an error if the file is not a valid SQLite database, or on
/// I/O / B-tree navigation failures.
#[allow(clippy::too_many_lines, clippy::similar_names)]
#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
pub async fn load_from_sqlite(cx: &Cx, path: &Path) -> Result<LoadedState> {
    let _record_profile_scope = enter_record_profile_scope(RecordProfileScope::CoreCompatPersist);
    let vfs = PlatformVfs::new();
    let pager = SimplePager::open_with_cx(cx, vfs, path, DEFAULT_PAGE_SIZE).await?;
    let mut txn = pager.begin(cx, TransactionMode::ReadOnly).await?;
    let page1 = txn.get_page(cx, PageNumber::ONE).await?;
    let (usable_size, page_size) = load_sqlite_cursor_sizes_from_page1(page1.as_ref())?;

    // Read sqlite_master entries from page 1.
    let master_entries = {
        let mut entries = Vec::new();
        let master_root = PageNumber::ONE;
        let mut cursor = fsqlite_btree::BtCursor::new(
            TransactionPageIo::new(&mut txn),
            master_root,
            usable_size,
            true,
        );
        configure_btree_cursor_page_size(&mut cursor, usable_size, page_size);

        if cursor.first(cx).await? {
            let mut payload_buf: Vec<u8> = Vec::new();
            loop {
                // bd-9e3xf.6: fuse rowid+payload via the cursor accessor
                // landed in 5459d778 — saves one parse_cell_at per row on
                // the schema replay path, where N can be the full count of
                // sqlite_master rows in the database.
                payload_buf.clear();
                let rowid = cursor.rowid_and_payload_into(cx, &mut payload_buf).await?;
                let values =
                    parse_record(&payload_buf).ok_or_else(|| FrankenError::DatabaseCorrupt {
                        detail: format!(
                            "sqlite_master row {rowid} payload is not a valid SQLite record"
                        ),
                    })?;
                entries.push(values);
                if !cursor.next(cx).await? {
                    break;
                }
            }
        }
        entries
    };
    validate_sqlite_schema_catalog_from_master_entries(&master_entries)?;

    // Parse each sqlite_master row.
    // Columns: type(0), name(1), tbl_name(2), rootpage(3), sql(4)
    let mut schema = Vec::new();
    let mut db = MemDatabase::new();

    for entry in &master_entries {
        if entry.len() < 5 {
            continue;
        }
        let entry_type = match &entry[0] {
            SqliteValue::Text(s) => s,
            _ => continue,
        };
        if !entry_type.eq_ignore_ascii_case("table") {
            continue; // Skip indexes, views, triggers for now.
        }

        let name = match &entry[1] {
            SqliteValue::Text(s) => s.clone(),
            _ => continue,
        };
        let create_sql = match &entry[4] {
            SqliteValue::Text(s) => s.clone(),
            _ => continue,
        };
        let is_virtual_sql = is_virtual_table_sql(&create_sql);
        let root_page_num = match &entry[3] {
            SqliteValue::Integer(n) => *n,
            SqliteValue::Null if is_virtual_sql => 0,
            _ => {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "sqlite_master table `{name}` has unsupported rootpage metadata"
                    ),
                });
            }
        };

        // Stock SQLite records virtual tables with rootpage=0. Those legacy
        // declarations have no materialized root page to load, so skip them.
        // Positive-rootpage virtual tables are real B-trees and must remain
        // visible on reopen just like ordinary tables.
        if root_page_num == 0 && is_virtual_table_sql(&create_sql) {
            continue;
        }
        let root_page_u32 = validate_sqlite_master_root_page(&name, root_page_num)?;

        // Parse the CREATE TABLE to extract column info and schema decorations.
        let implicit_constraint_plan = match parse_single_statement(&create_sql) {
            Some(Statement::CreateTable(create)) => {
                plan_implicit_constraint_indexes(&create, &name).map_err(|error| {
                    FrankenError::DatabaseCorrupt {
                        detail: format!(
                            "table `{name}` has invalid implicit constraint-index metadata: {error}"
                        ),
                    }
                })?
            }
            _ if is_virtual_table_sql(&create_sql) => Vec::new(),
            _ => {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "table `{name}` CREATE statement cannot be parsed for implicit constraint-index validation"
                    ),
                });
            }
        };
        let mut actual_implicit_indexes = Vec::new();
        for index_entry in &master_entries {
            if index_entry.len() < 5 {
                continue;
            }
            let is_index_for_table = matches!(
                (&index_entry[0], &index_entry[2]),
                (SqliteValue::Text(entry_type), SqliteValue::Text(entry_table))
                    if entry_type.eq_ignore_ascii_case("index")
                        && entry_table.eq_ignore_ascii_case(&name)
            );
            if !is_index_for_table || !matches!(&index_entry[4], SqliteValue::Null) {
                continue;
            }
            let SqliteValue::Text(index_name) = &index_entry[1] else {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: format!("table `{name}` has an implicit index row without a text name"),
                });
            };
            let SqliteValue::Integer(index_root_page) = &index_entry[3] else {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "implicit index `{index_name}` has a non-integer sqlite_master rootpage"
                    ),
                });
            };
            let validated_root = validate_sqlite_master_root_page(index_name, *index_root_page)?;
            let root_page =
                i32::try_from(validated_root).map_err(|_| FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "implicit index `{index_name}` rootpage {index_root_page} exceeds supported range"
                    ),
                })?;
            actual_implicit_indexes.push((index_name.to_string(), root_page));
        }
        let actual_implicit_index_names = actual_implicit_indexes
            .iter()
            .map(|(index_name, _)| index_name.clone())
            .collect::<Vec<_>>();
        validate_auxiliary_implicit_index_inventory(
            &name,
            &implicit_constraint_plan,
            &actual_implicit_index_names,
        )?;

        let columns = parse_columns_from_sqlite_master_sql(&create_sql);
        let mut indexes =
            extract_unique_constraint_indexes_from_sql(&create_sql, &name).map_err(|error| {
                FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "table `{name}` has invalid implicit constraint-index metadata: {error}"
                    ),
                }
            })?;
        let mut seen_implicit_roots = HashSet::new();
        for index in &mut indexes {
            let matching_roots = actual_implicit_indexes
                .iter()
                .filter(|(index_name, _)| index_name.eq_ignore_ascii_case(&index.name))
                .map(|(_, root_page)| *root_page)
                .collect::<Vec<_>>();
            let [root_page] = matching_roots.as_slice() else {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "implicit index `{}` on table `{name}` does not map to exactly one sqlite_master root",
                        index.name
                    ),
                });
            };
            if *root_page == i32::try_from(root_page_u32).unwrap_or(i32::MIN)
                || !seen_implicit_roots.insert(*root_page)
            {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "implicit index `{}` on table `{name}` has duplicate table/index rootpage {root_page}",
                        index.name
                    ),
                });
            }
            index.root_page = *root_page;
        }
        let primary_key_constraints = extract_primary_key_constraints_from_sql(&create_sql);
        let foreign_keys = extract_foreign_keys_from_sql(&create_sql, &columns);
        let check_constraints = extract_check_constraints_with_owners_from_sql(&create_sql);
        let num_columns = columns.len();
        let without_rowid = is_without_rowid_table_sql(&create_sql);
        let ipk_col_idx = columns.iter().position(|c| c.is_ipk);
        if without_rowid && columns.iter().any(|column| column.generated_expr.is_some()) {
            return Err(FrankenError::FunctionError(format!(
                "cannot load WITHOUT ROWID table `{name}` with generated columns without a complete physical-column map"
            )));
        }
        let without_rowid_layout = without_rowid
            .then(|| without_rowid_storage_layout(&name, &columns, &implicit_constraint_plan))
            .transpose()
            .map_err(|error| FrankenError::DatabaseCorrupt {
                detail: format!(
                    "table `{name}` has invalid WITHOUT ROWID storage metadata: {error}"
                ),
            })?;

        // Use the REAL root page from sqlite_master (5A.4: bd-1soh).
        let real_root_page =
            i32::try_from(root_page_u32).expect("validated root page must fit MemDatabase");
        db.create_table_at(real_root_page, num_columns);

        let table_name_for_err = name.to_string();
        schema.push(TableSchema {
            name: name.to_string(),
            root_page: real_root_page,
            columns,
            indexes: indexes.clone(),
            strict: is_strict_table_sql(&create_sql),
            without_rowid,
            primary_key_constraints,
            foreign_keys,
            check_constraints,
        });
        let current_table_schema = schema.last().ok_or_else(|| {
            FrankenError::Internal(format!(
                "compat loader lost table schema after registering `{table_name_for_err}`"
            ))
        })?;

        // Read all rows from this table's B-tree.
        let file_root =
            PageNumber::new(root_page_u32).expect("validated sqlite_master root page is positive");

        let mut cursor = fsqlite_btree::BtCursor::new(
            TransactionPageIo::new(&mut txn),
            file_root,
            usable_size,
            !without_rowid,
        );
        configure_btree_cursor_page_size(&mut cursor, usable_size, page_size);

        if let Some(mem_table) = db.tables.get_mut(&real_root_page) {
            let mut unique_groups = Vec::<(Vec<usize>, Vec<Option<String>>)>::new();
            if let Some(layout) = without_rowid_layout.as_ref() {
                unique_groups.push((
                    layout.primary_key_declared.clone(),
                    layout.primary_key_collations.clone(),
                ));
            }
            for (column_index, column) in current_table_schema.columns.iter().enumerate() {
                if column.unique
                    && !column.is_ipk
                    && !unique_groups
                        .iter()
                        .any(|(existing, _)| existing == &[column_index])
                {
                    unique_groups.push((vec![column_index], vec![column.collation.clone()]));
                }
            }
            for index in &indexes {
                if !index.is_unique || index.columns.is_empty() {
                    continue;
                }
                let (group, collations): (Vec<_>, Vec<_>) = index
                    .columns
                    .iter()
                    .enumerate()
                    .filter_map(|(term_idx, column_name)| {
                        current_table_schema
                            .columns
                            .iter()
                            .position(|column| column.name.eq_ignore_ascii_case(column_name))
                            .map(|column_index| {
                                (
                                    column_index,
                                    index.key_collations.get(term_idx).cloned().flatten(),
                                )
                            })
                    })
                    .unzip();
                if group.is_empty()
                    || group
                        .iter()
                        .all(|&column_index| current_table_schema.columns[column_index].is_ipk)
                    || unique_groups.iter().any(|(existing, _)| existing == &group)
                {
                    continue;
                }
                unique_groups.push((group, collations));
            }
            for (group, collations) in unique_groups {
                mem_table.add_unique_column_group_with_collations(group, collations);
            }
            if cursor.first(cx).await? {
                if without_rowid {
                    let mut synthetic_rowid = 1_i64;
                    let mut payload_buf: Vec<u8> = Vec::new();
                    loop {
                        payload_buf.clear();
                        cursor.payload_into(cx, &mut payload_buf).await?;
                        let mut values = parse_record(&payload_buf).ok_or_else(|| {
                            FrankenError::DatabaseCorrupt {
                                detail: format!(
                                    "WITHOUT ROWID table `{table_name_for_err}` payload is not a valid SQLite record"
                                ),
                            }
                        })?;
                        values = without_rowid_layout
                            .as_ref()
                            .ok_or_else(|| FrankenError::DatabaseCorrupt {
                                detail: format!(
                                    "WITHOUT ROWID table `{table_name_for_err}` has no physical storage layout"
                                ),
                            })?
                            .declared_values_from_storage(
                                &table_name_for_err,
                                &values,
                                &current_table_schema.columns,
                            )?;
                        mem_table.insert_row(synthetic_rowid, values);
                        synthetic_rowid = synthetic_rowid.saturating_add(1);
                        if !cursor.next(cx).await? {
                            break;
                        }
                    }
                    continue;
                }
                let mut payload_buf: Vec<u8> = Vec::new();
                loop {
                    // bd-9e3xf.6: fused accessor (5459d778) avoids a second
                    // parse_cell_at on every row of the legacy table-replay
                    // hot path used by file-backed schema hydration.
                    payload_buf.clear();
                    let rowid = cursor.rowid_and_payload_into(cx, &mut payload_buf).await?;
                    let mut values = parse_record(&payload_buf).ok_or_else(|| {
                        FrankenError::DatabaseCorrupt {
                            detail: format!(
                                "table `{table_name_for_err}` rowid {rowid} payload is not a valid SQLite record"
                            ),
                        }
                    })?;
                    inflate_loaded_table_row_values(
                        &mut values,
                        rowid,
                        &current_table_schema.columns,
                        if without_rowid { None } else { ipk_col_idx },
                        &table_name_for_err,
                    )?;
                    mem_table.insert_row(rowid, values);
                    if !cursor.next(cx).await? {
                        break;
                    }
                }
            }
        }
    }

    // Second pass: load explicit indexes from sqlite_master "index" entries.
    // Autoindexes from UNIQUE/PK constraints are already extracted from
    // CREATE TABLE SQL above; this handles `CREATE INDEX ...` definitions.
    for entry in &master_entries {
        if entry.len() < 5 {
            continue;
        }
        let entry_type = match &entry[0] {
            SqliteValue::Text(s) => s,
            _ => continue,
        };
        if !entry_type.eq_ignore_ascii_case("index") {
            continue;
        }
        let index_name = match &entry[1] {
            SqliteValue::Text(s) => s.to_string(),
            _ => continue,
        };
        let tbl_name = match &entry[2] {
            SqliteValue::Text(s) => s.to_string(),
            _ => continue,
        };
        let root_page_num = match &entry[3] {
            SqliteValue::Integer(n) => *n,
            _ => {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "sqlite_master index `{index_name}` has a non-integer rootpage"
                    ),
                });
            }
        };
        let create_sql = match &entry[4] {
            SqliteValue::Text(s) => s.to_string(),
            SqliteValue::Null => continue,
            _ => {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "sqlite_master index `{index_name}` has unsupported SQL metadata"
                    ),
                });
            }
        };

        let root_page_u32 = validate_sqlite_master_root_page(&index_name, root_page_num)?;
        let root_page_i32 =
            i32::try_from(root_page_u32).map_err(|_| FrankenError::DatabaseCorrupt {
                detail: format!(
                    "sqlite_master index `{index_name}` has rootpage {root_page_num} that exceeds supported range"
                ),
            })?;

        // Find the parent table in the schema.
        let Some(table) = schema
            .iter_mut()
            .find(|t| t.name.eq_ignore_ascii_case(&tbl_name))
        else {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "sqlite_master index `{index_name}` refers to missing table `{tbl_name}`"
                ),
            });
        };

        // Parse the CREATE INDEX SQL to extract column names, collations,
        // sort directions, and WHERE clause.
        let Some(mut idx_schema) =
            self::parse_create_index_sql_to_schema(&index_name, root_page_i32, &create_sql)
        else {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "sqlite_master index `{index_name}` SQL could not be reconstructed safely"
                ),
            });
        };
        for (key_position, column_name) in idx_schema.columns.iter().enumerate() {
            if idx_schema
                .key_collations
                .get(key_position)
                .is_some_and(Option::is_none)
                && let Some(column_collation) = table
                    .columns
                    .iter()
                    .find(|column| column.name.eq_ignore_ascii_case(column_name))
                    .and_then(|column| column.collation.clone())
            {
                idx_schema.key_collations[key_position] = Some(column_collation);
            }
        }
        // Only add if not already present (avoid duplicates with autoindexes).
        if !table.indexes.iter().any(|i| i.name == index_name) {
            table.indexes.push(idx_schema);
        }
    }

    // Reopened WITHOUT ROWID schemas must carry enough authoritative metadata
    // to reproduce every physical key exactly. In particular, a reserved
    // autoindex-looking name is implicit only when sqlite_master stored NULL
    // SQL for that exact table/index row; an explicit index with such a name
    // retains its declared PRIMARY KEY locator directions.
    for table in schema.iter().filter(|table| table.without_rowid) {
        table
            .without_rowid_table_layout()
            .map_err(|error| FrankenError::DatabaseCorrupt {
                detail: format!(
                    "WITHOUT ROWID table `{}` has invalid physical layout after schema reload: {error}",
                    table.name
                ),
            })?;
        for index in &table.indexes {
            let is_implicit = master_entries.iter().any(|entry| {
                entry.len() >= 5
                    && matches!(
                        (&entry[0], &entry[1], &entry[2], &entry[4]),
                        (
                            SqliteValue::Text(entry_type),
                            SqliteValue::Text(entry_name),
                            SqliteValue::Text(entry_table),
                            SqliteValue::Null,
                        ) if entry_type.eq_ignore_ascii_case("index")
                            && entry_name.eq_ignore_ascii_case(&index.name)
                            && entry_table.eq_ignore_ascii_case(&table.name)
                    )
            });
            table
                .without_rowid_index_layout(index, is_implicit)
                .map_err(|error| FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "WITHOUT ROWID index `{}.{}` has invalid physical layout after schema reload: {error}",
                        table.name, index.name
                    ),
                })?;
        }
    }

    // Read schema_cookie and change_counter from the database header (page 1).
    let (schema_cookie, change_counter) = {
        let header_buf = txn.get_page(cx, PageNumber::ONE).await?;
        let hdr = header_buf.as_ref();
        let cookie = if hdr.len() >= 44 {
            u32::from_be_bytes([hdr[40], hdr[41], hdr[42], hdr[43]])
        } else {
            0
        };
        let counter = if hdr.len() >= 28 {
            u32::from_be_bytes([hdr[24], hdr[25], hdr[26], hdr[27]])
        } else {
            0
        };
        (cookie, counter)
    };

    #[allow(clippy::cast_possible_wrap)]
    let master_row_count = master_entries.len() as i64;
    Ok(LoadedState {
        schema,
        db,
        master_row_count,
        schema_cookie,
        change_counter,
    })
}

// ── Helpers ─────────────────────────────────────────────────────────────

/// Initialize a page as an empty leaf table B-tree page (type 0x0D).
#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
async fn init_leaf_table_page(
    cx: &Cx,
    txn: &mut impl TransactionHandle,
    page_no: PageNumber,
    full_page_size: usize,
    usable_size: u32,
) -> Result<()> {
    let mut page = vec![0u8; full_page_size];
    page[0] = 0x0D; // Leaf table
    // cell_count = 0 (bytes 3..5)
    page[3..5].copy_from_slice(&0u16.to_be_bytes());
    // cell content area starts at end of page
    // SQLite encodes a content offset of 65536 as 0 in the 2-byte header field.
    // For all other valid page sizes (512..=32768), the value fits in u16 directly.
    let content_start: u16 = if usable_size == 65536 {
        0
    } else {
        u16::try_from(usable_size).map_err(|_| {
            FrankenError::internal(format!(
                "usable_size {usable_size} does not fit in u16 and is not 65536"
            ))
        })?
    };
    page[5..7].copy_from_slice(&content_start.to_be_bytes());
    txn.write_page(cx, page_no, &page).await
}

#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
async fn init_leaf_index_page(
    cx: &Cx,
    txn: &mut impl TransactionHandle,
    page_no: PageNumber,
    full_page_size: usize,
    usable_size: u32,
) -> Result<()> {
    let mut page = vec![0u8; full_page_size];
    page[0] = 0x0A; // Leaf index (vs 0x0D for leaf table)
    page[3..5].copy_from_slice(&0u16.to_be_bytes());
    let content_start: u16 = if usable_size == 65536 {
        0
    } else {
        u16::try_from(usable_size).map_err(|_| {
            FrankenError::internal(format!(
                "usable_size {usable_size} does not fit in u16 and is not 65536"
            ))
        })?
    };
    page[5..7].copy_from_slice(&content_start.to_be_bytes());
    txn.write_page(cx, page_no, &page).await
}

/// Parse a `CREATE INDEX` SQL string into an `IndexSchema`.
/// Returns `None` if the SQL cannot be parsed.
fn parse_create_index_sql_to_schema(
    index_name: &str,
    root_page: i32,
    sql: &str,
) -> Option<IndexSchema> {
    if let Some(Statement::CreateIndex(create)) = parse_single_statement(sql) {
        return Some(create_index_statement_to_index_schema(
            index_name, root_page, &create,
        ));
    }

    // Simple regex-free parser: look for "ON table_name (col1, col2 COLLATE NOCASE DESC)"
    // while preserving quoted names and comments inside the indexed term list.
    let keyword_tokens = unquoted_sql_keyword_tokens(sql);
    let is_unique = unquoted_tokens_contain_phrase(&keyword_tokens, &["CREATE", "UNIQUE", "INDEX"]);
    // Find the indexed-term list between the unquoted '(' after ON and its
    // matching ')'.
    let on_pos = find_unquoted_sql_keyword(sql, "ON")?;
    let after_on_pos = on_pos + "ON".len();
    let paren_start = after_on_pos + find_unquoted_sql_char(&sql[after_on_pos..], '(')?;
    let paren_end = find_matching_sql_paren(sql, paren_start)?;
    let col_list = &sql[paren_start + 1..paren_end];

    let mut columns = Vec::new();
    let mut collations = Vec::new();
    let mut directions = Vec::new();

    for part in split_top_level_csv_items(col_list) {
        let (col_name, remainder) = parse_column_name_and_remainder(&part)?;
        columns.push(col_name);
        collations.push(extract_collation_name(remainder));
        directions.push(extract_index_term_direction(remainder));
    }

    // WHERE clause for partial indexes (everything after the closing paren).
    let after_paren = trim_leading_sql_space_and_comments(&sql[paren_end + 1..]);
    let where_clause = if collect_unquoted_sql_keyword_tokens(after_paren)
        .first()
        .is_some_and(|(token, start)| token == "WHERE" && *start == 0)
    {
        let expr = trim_leading_sql_space_and_comments(&after_paren["WHERE".len()..]);
        Some(expr.to_owned())
    } else {
        None
    };

    Some(IndexSchema {
        name: index_name.to_owned(),
        root_page,
        columns,
        key_expressions: Vec::new(),
        key_sort_directions: directions,
        where_clause,
        is_unique,
        key_collations: collations,
        conflict_action: None,
    })
}

fn create_index_statement_to_index_schema(
    index_name: &str,
    root_page: i32,
    create: &fsqlite_ast::CreateIndexStatement,
) -> IndexSchema {
    let normalized_terms = create
        .columns
        .iter()
        .map(|indexed| {
            Some((
                indexed_column_name(indexed)?.to_owned(),
                normalized_indexed_column_collation(indexed),
            ))
        })
        .collect::<Option<Vec<_>>>();
    let (columns, key_expressions, key_collations) =
        if let Some(normalized_terms) = normalized_terms {
            (
                normalized_terms
                    .iter()
                    .map(|(column_name, _)| column_name.clone())
                    .collect(),
                Vec::new(),
                normalized_terms
                    .into_iter()
                    .map(|(_, collation)| collation)
                    .collect(),
            )
        } else {
            (
                Vec::new(),
                create
                    .columns
                    .iter()
                    .map(|indexed| indexed.expr.to_string())
                    .collect(),
                create
                    .columns
                    .iter()
                    .map(normalized_indexed_column_collation)
                    .collect(),
            )
        };

    IndexSchema {
        name: index_name.to_owned(),
        root_page,
        columns,
        key_expressions,
        key_sort_directions: create
            .columns
            .iter()
            .map(|indexed| indexed.direction.unwrap_or(SortDirection::Asc))
            .collect(),
        where_clause: create.where_clause.as_ref().map(ToString::to_string),
        is_unique: create.unique,
        key_collations,
        conflict_action: None,
    }
}

fn normalized_indexed_column_collation(indexed: &IndexedColumn) -> Option<String> {
    indexed_column_collation(indexed).map(|collation| collation.to_ascii_uppercase())
}

fn extract_index_term_direction(remainder: &str) -> SortDirection {
    let collation_name_range = find_collation_name_range(remainder);
    let mut direction = SortDirection::Asc;
    for (token, start) in collect_unquoted_sql_keyword_tokens(remainder) {
        if collation_name_range
            .as_ref()
            .is_some_and(|range| range.contains(&start))
        {
            continue;
        }
        match token.as_str() {
            "DESC" => direction = SortDirection::Desc,
            "ASC" => direction = SortDirection::Asc,
            _ => {}
        }
    }
    direction
}

fn quote_identifier(identifier: &str) -> String {
    let escaped = identifier.replace('"', "\"\"");
    format!("\"{escaped}\"")
}

fn append_fk_reference_clause(sql: &mut String, fk: &FkDef) {
    use std::fmt::Write as _;

    let _ = write!(sql, " REFERENCES {}", quote_identifier(&fk.parent_table));
    if !fk.parent_columns.is_empty() {
        let parent_columns = fk
            .parent_columns
            .iter()
            .map(|column_name| quote_identifier(column_name))
            .collect::<Vec<_>>()
            .join(", ");
        let _ = write!(sql, "({parent_columns})");
    }
    if fk.on_delete != FkActionType::NoAction {
        let _ = write!(sql, " ON DELETE {}", fk_action_sql(fk.on_delete));
    }
    if fk.on_update != FkActionType::NoAction {
        let _ = write!(sql, " ON UPDATE {}", fk_action_sql(fk.on_update));
    }
    if fk.deferred {
        sql.push_str(" DEFERRABLE INITIALLY DEFERRED");
    }
}

/// Reconstruct a `CREATE TABLE` statement from a `TableSchema`.
pub(crate) fn build_create_table_sql(table: &TableSchema) -> String {
    build_create_table_sql_with_implicit_index_predicate(table, |index| {
        parse_autoindex_ordinal(&index.name, &table.name).is_some()
    })
}

fn build_create_table_sql_with_implicit_index_predicate<F>(
    table: &TableSchema,
    is_implicit_autoindex: F,
) -> String
where
    F: Fn(&IndexSchema) -> bool,
{
    use std::fmt::Write as _;
    let mut sql = format!("CREATE TABLE {} (", quote_identifier(&table.name));
    let is_single_column_primary_key = |column_name: &str| {
        table
            .primary_key_constraints
            .iter()
            .any(|pk| pk.len() == 1 && pk[0].eq_ignore_ascii_case(column_name))
    };
    let collations_equivalent = |left: Option<&str>, right: Option<&str>| {
        left.unwrap_or("BINARY")
            .eq_ignore_ascii_case(right.unwrap_or("BINARY"))
    };
    let primary_key_matches_index = |index: &fsqlite_vdbe::codegen::IndexSchema| {
        table.primary_key_constraints.iter().any(|pk| {
            pk.len() == index.columns.len()
                && pk
                    .iter()
                    .zip(index.columns.iter())
                    .all(|(lhs, rhs): (&String, &String)| lhs.eq_ignore_ascii_case(rhs))
                && (!table.without_rowid
                    || (0..pk.len()).all(|position| {
                        index.key_term_descending(position)
                            == matches!(
                                pk.key_sort_directions.get(position),
                                Some(SortDirection::Desc)
                            )
                            && collations_equivalent(
                                index.key_term_collation(position),
                                pk.key_collations
                                    .get(position)
                                    .and_then(|collation| collation.as_deref()),
                            )
                    }))
        })
    };
    for (i, col) in table.columns.iter().enumerate() {
        if i > 0 {
            sql.push_str(", ");
        }
        sql.push_str(&quote_identifier(&col.name));
        if let Some(type_kw) = col.type_name.as_deref() {
            let _ = write!(sql, " {type_kw}");
        }
        if col.is_ipk {
            sql.push_str(" PRIMARY KEY");
        }
        if col.notnull && !col.is_ipk {
            sql.push_str(" NOT NULL");
        }
        if col.unique && !col.is_ipk && !is_single_column_primary_key(&col.name) {
            sql.push_str(" UNIQUE");
        }
        if let Some(ref default) = col.default_value {
            sql.push_str(" DEFAULT ");
            sql.push_str(default);
        }
        if let Some(ref collation) = col.collation {
            sql.push_str(" COLLATE ");
            sql.push_str(&quote_identifier(collation));
        }
        if let Some(ref gen_expr) = col.generated_expr {
            sql.push_str(" GENERATED ALWAYS AS (");
            sql.push_str(gen_expr);
            sql.push(')');
            if col.generated_stored == Some(true) {
                sql.push_str(" STORED");
            } else {
                sql.push_str(" VIRTUAL");
            }
        }
        for check in table.check_constraints.iter().filter(|check| {
            check
                .owner_column
                .as_deref()
                .is_some_and(|owner| owner.eq_ignore_ascii_case(&col.name))
        }) {
            let _ = write!(sql, " CHECK({})", check.expr);
        }
        for fk in table.foreign_keys.iter().filter(|fk| {
            fk.owner_column
                .as_deref()
                .is_some_and(|owner| owner.eq_ignore_ascii_case(&col.name))
        }) {
            append_fk_reference_clause(&mut sql, fk);
        }
    }
    // Emit PRIMARY KEY constraints BEFORE UNIQUE constraints.  Stock SQLite
    // assigns autoindex ordinals (sqlite_autoindex_T_N) in the order they
    // appear: first column-level PK, then table-level PK, then column-level
    // UNIQUE, then table-level UNIQUE.  If we emit UNIQUE before PRIMARY KEY,
    // the ordinal-to-definition mapping in
    // `infer_implicit_index_definition_from_master_entries` will assign the
    // wrong columns to autoindexes, corrupting the schema on reload (#239).
    for pk in &table.primary_key_constraints {
        if pk.len() == 1
            && table
                .columns
                .iter()
                .any(|column| column.is_ipk && column.name.eq_ignore_ascii_case(&pk[0]))
        {
            continue;
        }
        let cols = pk
            .columns
            .iter()
            .enumerate()
            .map(|(position, name)| {
                let mut term = quote_identifier(name);
                if let Some(collation) = pk
                    .key_collations
                    .get(position)
                    .and_then(|collation| collation.as_deref())
                {
                    let _ = write!(term, " COLLATE {}", quote_identifier(collation));
                }
                if matches!(
                    pk.key_sort_directions.get(position),
                    Some(SortDirection::Desc)
                ) {
                    term.push_str(" DESC");
                }
                term
            })
            .collect::<Vec<_>>()
            .join(", ");
        let _ = write!(sql, ", PRIMARY KEY ({cols})");
    }
    for index in &table.indexes {
        if !index.is_unique || index.columns.is_empty() || primary_key_matches_index(index) {
            continue;
        }
        // Only emit table-level UNIQUE for autoindexes.  Explicitly-named
        // indexes (e.g. `idx_issues_external_ref_unique`) are written as
        // separate CREATE INDEX entries in sqlite_master; emitting them
        // here as well would create a phantom sqlite_autoindex that stock
        // SQLite tries to populate, causing "wrong # of entries" errors.
        if !is_implicit_autoindex(index) {
            continue;
        }
        if index.columns.len() == 1
            && table.columns.iter().any(|column| {
                column.unique
                    && !column.is_ipk
                    && column.name.eq_ignore_ascii_case(&index.columns[0])
            })
        {
            continue;
        }
        let cols = index
            .columns
            .iter()
            .enumerate()
            .map(|(position, name)| {
                let mut term = quote_identifier(name);
                if let Some(collation) = index.key_term_collation(position) {
                    let _ = write!(term, " COLLATE {}", quote_identifier(collation));
                }
                if index.key_term_descending(position) {
                    term.push_str(" DESC");
                }
                term
            })
            .collect::<Vec<_>>()
            .join(", ");
        let _ = write!(sql, ", UNIQUE ({cols})");
    }
    for fk in table
        .foreign_keys
        .iter()
        .filter(|fk| fk.owner_column.is_none())
    {
        let child_columns = fk
            .child_columns
            .iter()
            .filter_map(|&column_index| table.columns.get(column_index))
            .map(|column| quote_identifier(&column.name))
            .collect::<Vec<_>>();
        if child_columns.is_empty() {
            continue;
        }
        let _ = write!(sql, ", FOREIGN KEY({})", child_columns.join(", "));
        append_fk_reference_clause(&mut sql, fk);
    }
    for check in table
        .check_constraints
        .iter()
        .filter(|check| check.owner_column.is_none())
    {
        let _ = write!(sql, ", CHECK({})", check.expr);
    }
    sql.push(')');
    let mut table_options = Vec::new();
    if table.without_rowid {
        table_options.push("WITHOUT ROWID");
    }
    if table.strict {
        table_options.push("STRICT");
    }
    if !table_options.is_empty() {
        sql.push(' ');
        sql.push_str(&table_options.join(", "));
    }
    sql
}

const fn fk_action_sql(action: FkActionType) -> &'static str {
    match action {
        FkActionType::NoAction => "NO ACTION",
        FkActionType::Restrict => "RESTRICT",
        FkActionType::SetNull => "SET NULL",
        FkActionType::SetDefault => "SET DEFAULT",
        FkActionType::Cascade => "CASCADE",
    }
}

pub(crate) fn extract_primary_key_constraints_from_sql(sql: &str) -> Vec<PrimaryKeyConstraint> {
    let Some(Statement::CreateTable(create)) = parse_single_statement(sql) else {
        return Vec::new();
    };
    let CreateTableBody::Columns {
        columns,
        constraints,
    } = &create.body
    else {
        return Vec::new();
    };

    let mut primary_keys = columns
        .iter()
        .filter_map(|column| {
            column.constraints.iter().find_map(|constraint| {
                let ColumnConstraintKind::PrimaryKey { direction, .. } = &constraint.kind else {
                    return None;
                };
                Some(PrimaryKeyConstraint::new(
                    vec![column.name.clone()],
                    vec![direction.unwrap_or(SortDirection::Asc)],
                    vec![effective_column_collation(columns, &column.name)],
                ))
            })
        })
        .collect::<Vec<_>>();

    primary_keys.extend(constraints.iter().filter_map(|constraint| {
        let TableConstraintKind::PrimaryKey {
            columns: indexed_columns,
            ..
        } = &constraint.kind
        else {
            return None;
        };
        let terms = indexed_columns
            .iter()
            .map(|indexed| {
                let column_name = indexed_column_name(indexed)?.to_owned();
                let explicit_collation = normalized_indexed_column_collation(indexed);
                Some((
                    column_name.clone(),
                    indexed.direction.unwrap_or(SortDirection::Asc),
                    explicit_collation
                        .or_else(|| effective_column_collation(columns, &column_name)),
                ))
            })
            .collect::<Option<Vec<_>>>()?;
        if terms.is_empty() {
            return None;
        }
        Some(PrimaryKeyConstraint::new(
            terms.iter().map(|(column, _, _)| column.clone()).collect(),
            terms.iter().map(|(_, direction, _)| *direction).collect(),
            terms
                .into_iter()
                .map(|(_, _, collation)| collation)
                .collect(),
        ))
    }));

    primary_keys
}

/// Physical storage selected for an implicit PRIMARY KEY or UNIQUE index.
///
/// A `WITHOUT ROWID` table stores its PRIMARY KEY in the table B-tree itself.
/// SQLite nevertheless assigns that logical key an autoindex ordinal, so the
/// auxiliary indexes persisted in `sqlite_schema` can have sparse names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ImplicitConstraintIndexBacking {
    /// A separate index B-tree with its own `sqlite_schema` row.
    AuxiliaryBtree,
    /// The `WITHOUT ROWID` table B-tree; no separate schema row or root page.
    WithoutRowidTableRoot,
}

/// Authoritative logical plan for one implicit constraint index.
///
/// `ordinal` and `name` describe SQLite's logical autoindex namespace. Callers
/// must consult `backing` before expecting a corresponding `sqlite_schema`
/// index row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ImplicitConstraintIndexSpec {
    pub(crate) ordinal: usize,
    pub(crate) name: String,
    pub(crate) columns: Vec<String>,
    pub(crate) key_sort_directions: Vec<SortDirection>,
    pub(crate) key_collations: Vec<Option<String>>,
    pub(crate) conflict_action: Option<ConflictAction>,
    pub(crate) is_primary_key: bool,
    pub(crate) backing: ImplicitConstraintIndexBacking,
}

#[derive(Debug, Clone)]
struct ImplicitConstraintIndexCandidate {
    columns: Vec<String>,
    key_sort_directions: Vec<SortDirection>,
    key_collations: Vec<Option<String>>,
    conflict_action: Option<ConflictAction>,
    is_primary_key: bool,
}

impl ImplicitConstraintIndexCandidate {
    fn backing(&self, without_rowid: bool) -> ImplicitConstraintIndexBacking {
        if self.is_primary_key && without_rowid {
            ImplicitConstraintIndexBacking::WithoutRowidTableRoot
        } else {
            ImplicitConstraintIndexBacking::AuxiliaryBtree
        }
    }
}

fn effective_column_collation(
    columns: &[fsqlite_ast::ColumnDef],
    column_name: &str,
) -> Option<String> {
    columns
        .iter()
        .find(|column| column.name.eq_ignore_ascii_case(column_name))
        .and_then(|column| {
            column
                .constraints
                .iter()
                .filter_map(|constraint| {
                    if let ColumnConstraintKind::Collate(collation) = &constraint.kind {
                        Some(collation.clone())
                    } else {
                        None
                    }
                })
                .next_back()
        })
}

fn implicit_constraint_collations_equivalent(
    left: &[Option<String>],
    right: &[Option<String>],
) -> bool {
    left.len() == right.len()
        && left.iter().zip(right).all(|(left, right)| {
            left.as_deref()
                .unwrap_or("BINARY")
                .eq_ignore_ascii_case(right.as_deref().unwrap_or("BINARY"))
        })
}

fn implicit_constraint_candidates_equivalent(
    left: &ImplicitConstraintIndexSpec,
    right: &ImplicitConstraintIndexCandidate,
) -> bool {
    left.columns.len() == right.columns.len()
        && left
            .columns
            .iter()
            .zip(&right.columns)
            .all(|(left, right)| left.eq_ignore_ascii_case(right))
        && implicit_constraint_collations_equivalent(&left.key_collations, &right.key_collations)
}

fn merge_implicit_constraint_conflict_actions(
    left: Option<ConflictAction>,
    right: Option<ConflictAction>,
) -> Result<Option<ConflictAction>> {
    match (left, right) {
        (None, right) => Ok(right),
        (left, None) => Ok(left),
        (Some(left), Some(right)) if left == right => Ok(Some(left)),
        (Some(_), Some(_)) => Err(FrankenError::FunctionError(
            "conflicting ON CONFLICT clauses specified".to_owned(),
        )),
    }
}

fn insert_implicit_constraint_candidate(
    specs: &mut Vec<ImplicitConstraintIndexSpec>,
    table_name: &str,
    without_rowid: bool,
    candidate: ImplicitConstraintIndexCandidate,
) -> Result<()> {
    if let Some(existing) = specs
        .iter_mut()
        .find(|existing| implicit_constraint_candidates_equivalent(existing, &candidate))
    {
        existing.conflict_action = merge_implicit_constraint_conflict_actions(
            existing.conflict_action,
            candidate.conflict_action,
        )?;
        if candidate.is_primary_key {
            existing.is_primary_key = true;
            existing.backing = candidate.backing(without_rowid);
        }
        return Ok(());
    }

    let ordinal = specs.len() + 1;
    let backing = candidate.backing(without_rowid);
    specs.push(ImplicitConstraintIndexSpec {
        ordinal,
        name: format!("sqlite_autoindex_{table_name}_{ordinal}"),
        columns: candidate.columns,
        key_sort_directions: candidate.key_sort_directions,
        key_collations: candidate.key_collations,
        conflict_action: candidate.conflict_action,
        is_primary_key: candidate.is_primary_key,
        backing,
    });
    Ok(())
}

fn column_has_exact_integer_type(column: &fsqlite_ast::ColumnDef) -> bool {
    column
        .type_name
        .as_ref()
        .is_some_and(|type_name| type_name.name.eq_ignore_ascii_case("INTEGER"))
}

fn table_primary_key_is_deferred_integer(
    columns: &[fsqlite_ast::ColumnDef],
    indexed_columns: &[IndexedColumn],
) -> bool {
    if indexed_columns.len() != 1 {
        return false;
    }
    let Some(column_name) = indexed_column_name(&indexed_columns[0]) else {
        return false;
    };
    columns
        .iter()
        .find(|column| column.name.eq_ignore_ascii_case(column_name))
        .is_some_and(column_has_exact_integer_type)
}

/// Plan all implicit PRIMARY KEY and UNIQUE indexes for a parsed table.
///
/// The plan deliberately models SQLite's less-obvious rules:
///
/// * constraint order determines the autoindex ordinal;
/// * equivalent constraints share the first slot, direction, and spelling;
/// * an equivalent PRIMARY KEY promotes that slot to the table root for a
///   `WITHOUT ROWID` table;
/// * column `INTEGER PRIMARY KEY` with no direction or `ASC`, plus every
///   single-column table-level INTEGER PRIMARY KEY, is handled after ordinary
///   constraints. Rowid tables discard it; `WITHOUT ROWID` tables merge it at
///   the end.
pub(crate) fn plan_implicit_constraint_indexes(
    create: &CreateTableStatement,
    table_name: &str,
) -> Result<Vec<ImplicitConstraintIndexSpec>> {
    let CreateTableBody::Columns {
        columns,
        constraints,
    } = &create.body
    else {
        return Ok(Vec::new());
    };

    let mut specs = Vec::new();
    let mut deferred_integer_primary_keys = Vec::new();

    for column in columns {
        let effective_collation = effective_column_collation(columns, &column.name);
        for constraint in &column.constraints {
            let (is_primary_key, direction, conflict_action) = match &constraint.kind {
                ColumnConstraintKind::PrimaryKey {
                    direction,
                    conflict,
                    ..
                } => (true, direction.unwrap_or(SortDirection::Asc), *conflict),
                ColumnConstraintKind::Unique { conflict } => (false, SortDirection::Asc, *conflict),
                _ => continue,
            };
            let candidate = ImplicitConstraintIndexCandidate {
                columns: vec![column.name.clone()],
                key_sort_directions: vec![direction],
                key_collations: vec![effective_collation.clone()],
                conflict_action,
                is_primary_key,
            };
            let deferred_integer_primary_key = is_primary_key
                && column_has_exact_integer_type(column)
                && matches!(
                    &constraint.kind,
                    ColumnConstraintKind::PrimaryKey {
                        direction: None | Some(SortDirection::Asc),
                        ..
                    }
                );
            if deferred_integer_primary_key {
                deferred_integer_primary_keys.push(candidate);
            } else {
                insert_implicit_constraint_candidate(
                    &mut specs,
                    table_name,
                    create.without_rowid,
                    candidate,
                )?;
            }
        }
    }

    for constraint in constraints {
        let (indexed_columns, is_primary_key, conflict_action) = match &constraint.kind {
            TableConstraintKind::PrimaryKey { columns, conflict } => (columns, true, *conflict),
            TableConstraintKind::Unique { columns, conflict } => (columns, false, *conflict),
            _ => continue,
        };
        let columns_and_collations = indexed_columns
            .iter()
            .map(|indexed_column| {
                let column_name = indexed_column_name(indexed_column)
                    .ok_or_else(|| {
                        FrankenError::FunctionError(
                            "expressions prohibited in PRIMARY KEY and UNIQUE constraints"
                                .to_owned(),
                        )
                    })?
                    .to_owned();
                let Some(column) = columns
                    .iter()
                    .find(|column| column.name.eq_ignore_ascii_case(&column_name))
                else {
                    return Err(FrankenError::FunctionError(format!(
                        "no such column: {column_name}"
                    )));
                };
                let collation = indexed_column_collation(indexed_column)
                    .or_else(|| effective_column_collation(columns, &column.name));
                Ok((column_name, collation))
            })
            .collect::<Result<Vec<_>>>()?;
        if columns_and_collations.is_empty() {
            continue;
        }
        let candidate = ImplicitConstraintIndexCandidate {
            columns: columns_and_collations
                .iter()
                .map(|(column, _)| column.clone())
                .collect(),
            key_sort_directions: indexed_columns
                .iter()
                .map(|indexed_column| indexed_column.direction.unwrap_or(SortDirection::Asc))
                .collect(),
            key_collations: columns_and_collations
                .into_iter()
                .map(|(_, collation)| collation)
                .collect(),
            conflict_action,
            is_primary_key,
        };
        if is_primary_key && table_primary_key_is_deferred_integer(columns, indexed_columns) {
            deferred_integer_primary_keys.push(candidate);
        } else {
            insert_implicit_constraint_candidate(
                &mut specs,
                table_name,
                create.without_rowid,
                candidate,
            )?;
        }
    }

    if create.without_rowid {
        for candidate in deferred_integer_primary_keys {
            insert_implicit_constraint_candidate(
                &mut specs,
                table_name,
                create.without_rowid,
                candidate,
            )?;
        }
    }

    Ok(specs)
}

/// Validate the exact set of separately-backed implicit indexes for one table.
///
/// The caller must pass only `sqlite_schema` index rows whose SQL is NULL.
/// Hidden `WITHOUT ROWID` PRIMARY KEY specs are intentionally absent from the
/// expected set, while their ordinal remains reserved in each spec's name.
pub(crate) fn validate_auxiliary_implicit_index_inventory(
    table_name: &str,
    specs: &[ImplicitConstraintIndexSpec],
    actual_names: &[String],
) -> Result<()> {
    let mut expected = specs
        .iter()
        .filter(|spec| spec.backing == ImplicitConstraintIndexBacking::AuxiliaryBtree)
        .map(|spec| spec.name.to_ascii_lowercase())
        .collect::<Vec<_>>();
    let mut actual = actual_names
        .iter()
        .map(|name| name.to_ascii_lowercase())
        .collect::<Vec<_>>();
    expected.sort_unstable();
    actual.sort_unstable();
    if expected == actual {
        return Ok(());
    }

    let missing = expected
        .iter()
        .filter(|name| !actual.contains(name))
        .cloned()
        .collect::<Vec<_>>();
    let unexpected = actual
        .iter()
        .filter(|name| !expected.contains(name))
        .cloned()
        .collect::<Vec<_>>();
    let duplicate = actual
        .windows(2)
        .filter(|pair| pair[0] == pair[1])
        .map(|pair| pair[0].clone())
        .collect::<Vec<_>>();
    Err(FrankenError::DatabaseCorrupt {
        detail: format!(
            "table `{table_name}` implicit constraint-index inventory mismatch: missing={missing:?}, unexpected={unexpected:?}, duplicate={duplicate:?}"
        ),
    })
}

fn extract_unique_constraint_indexes_from_sql(
    sql: &str,
    table_name: &str,
) -> Result<Vec<IndexSchema>> {
    let Some(Statement::CreateTable(create)) = parse_single_statement(sql) else {
        return Ok(Vec::new());
    };
    Ok(plan_implicit_constraint_indexes(&create, table_name)?
        .into_iter()
        .filter(|spec| spec.backing == ImplicitConstraintIndexBacking::AuxiliaryBtree)
        .map(|spec| IndexSchema {
            name: spec.name,
            root_page: 0,
            columns: spec.columns,
            key_expressions: Vec::new(),
            key_sort_directions: spec.key_sort_directions,
            where_clause: None,
            is_unique: true,
            key_collations: spec.key_collations,
            conflict_action: spec.conflict_action,
        })
        .collect())
}

/// Canonical SQLite record layout for one `WITHOUT ROWID` table root.
///
/// SQLite stores the physical PRIMARY KEY terms first, in declared PRIMARY KEY
/// order, followed by every non-PK column in table declaration order. Repeated
/// terms with the same column and effective collation collapse to the first
/// term, while a same-column term with a distinct collation remains a separate
/// physical field. `MemDatabase` keeps row images in table declaration order,
/// so compat persistence and loading must apply this mapping in opposite
/// directions.
#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
#[derive(Debug, Clone)]
struct WithoutRowidStorageLayout {
    storage_to_declared: Vec<usize>,
    primary_key_declared: Vec<usize>,
    primary_key_sort_directions: Vec<SortDirection>,
    primary_key_collations: Vec<Option<String>>,
}

#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
impl WithoutRowidStorageLayout {
    fn primary_key_len(&self) -> usize {
        self.primary_key_declared.len()
    }

    fn table_cursor_desc_flags(&self) -> Vec<bool> {
        self.primary_key_sort_directions
            .iter()
            .map(|direction| *direction == SortDirection::Desc)
            .chain(std::iter::repeat_n(
                false,
                self.storage_to_declared.len() - self.primary_key_len(),
            ))
            .collect()
    }

    fn table_cursor_collations(&self) -> Vec<Option<String>> {
        self.primary_key_collations
            .iter()
            .cloned()
            .chain(std::iter::repeat_n(
                None,
                self.storage_to_declared.len() - self.primary_key_len(),
            ))
            .collect()
    }

    fn storage_values_from_declared(
        &self,
        table_name: &str,
        declared_values: &[SqliteValue],
    ) -> Result<Vec<SqliteValue>> {
        if declared_values.len() > self.storage_to_declared.len() {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "WITHOUT ROWID table `{table_name}` in-memory row has {} columns; schema declares {}",
                    declared_values.len(),
                    self.storage_to_declared.len()
                ),
            });
        }
        for &declared_idx in &self.primary_key_declared {
            let Some(value) = declared_values.get(declared_idx) else {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "WITHOUT ROWID table `{table_name}` in-memory row omits PRIMARY KEY column at declaration slot {declared_idx}"
                    ),
                });
            };
            if matches!(value, SqliteValue::Null) {
                return Err(FrankenError::NotNullViolation {
                    column: format!("{table_name}.PRIMARY KEY"),
                });
            }
        }

        Ok(self
            .storage_to_declared
            .iter()
            .filter_map(|&declared_idx| declared_values.get(declared_idx).cloned())
            .collect())
    }

    fn declared_values_from_storage(
        &self,
        table_name: &str,
        storage_values: &[SqliteValue],
        columns: &[ColumnInfo],
    ) -> Result<Vec<SqliteValue>> {
        if storage_values.len() > self.storage_to_declared.len() {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "WITHOUT ROWID table `{table_name}` key record has {} columns; schema declares {}",
                    storage_values.len(),
                    self.storage_to_declared.len()
                ),
            });
        }
        if storage_values.len() < self.primary_key_len() {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "WITHOUT ROWID table `{table_name}` key record has {} columns; PRIMARY KEY requires {}",
                    storage_values.len(),
                    self.primary_key_len()
                ),
            });
        }

        let mut declared_values = columns
            .iter()
            .map(|column| {
                column
                    .default_value
                    .as_deref()
                    .map_or(SqliteValue::Null, parse_loaded_column_default_value)
            })
            .collect::<Vec<_>>();
        let mut seen_declared_values = vec![None; columns.len()];
        for (storage_idx, value) in storage_values.iter().enumerate() {
            let declared_idx = self.storage_to_declared[storage_idx];
            if let Some(previous) = seen_declared_values[declared_idx]
                && previous != value
            {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "WITHOUT ROWID table `{table_name}` stores inconsistent copies of repeated PRIMARY KEY column `{}`",
                        columns[declared_idx].name
                    ),
                });
            }
            seen_declared_values[declared_idx] = Some(value);
            declared_values[declared_idx] = value.clone();
        }
        for &declared_idx in &self.primary_key_declared {
            if matches!(declared_values[declared_idx], SqliteValue::Null) {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "WITHOUT ROWID table `{table_name}` stores NULL in PRIMARY KEY column `{}`",
                        columns[declared_idx].name
                    ),
                });
            }
        }
        Ok(declared_values)
    }
}

#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
#[derive(Debug, Clone)]
struct WithoutRowidSecondaryIndexLayout {
    logical_declared: Vec<usize>,
    locator_declared: Vec<usize>,
    cursor_desc_flags: Vec<bool>,
    cursor_collations: Vec<Option<String>>,
}

#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
#[derive(Debug, Clone)]
struct PreparedWithoutRowidPersistence {
    table_layout: WithoutRowidStorageLayout,
    secondary_layouts: Vec<WithoutRowidSecondaryIndexLayout>,
}

#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
fn without_rowid_storage_layout(
    table_name: &str,
    columns: &[ColumnInfo],
    implicit_specs: &[ImplicitConstraintIndexSpec],
) -> Result<WithoutRowidStorageLayout> {
    let table_root_specs = implicit_specs
        .iter()
        .filter(|spec| {
            spec.backing == ImplicitConstraintIndexBacking::WithoutRowidTableRoot
                && spec.is_primary_key
        })
        .collect::<Vec<_>>();
    let [primary_key] = table_root_specs.as_slice() else {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "WITHOUT ROWID table `{table_name}` must have exactly one table-root PRIMARY KEY plan; found {}",
                table_root_specs.len()
            ),
        });
    };
    if primary_key.columns.is_empty() {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!("WITHOUT ROWID table `{table_name}` has an empty PRIMARY KEY"),
        });
    }
    if primary_key.key_sort_directions.len() != primary_key.columns.len()
        || primary_key.key_collations.len() != primary_key.columns.len()
    {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "WITHOUT ROWID table `{table_name}` PRIMARY KEY metadata width does not match its {} columns",
                primary_key.columns.len()
            ),
        });
    }

    let mut seen_declared_names = HashSet::with_capacity(columns.len());
    for column in columns {
        if !seen_declared_names.insert(column.name.to_ascii_lowercase()) {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "WITHOUT ROWID table `{table_name}` has duplicate column `{}`",
                    column.name
                ),
            });
        }
    }

    let mut primary_key_declared = Vec::with_capacity(primary_key.columns.len());
    let mut primary_key_sort_directions = Vec::with_capacity(primary_key.columns.len());
    let mut primary_key_collations: Vec<Option<String>> =
        Vec::with_capacity(primary_key.columns.len());
    for (pk_position, column_name) in primary_key.columns.iter().enumerate() {
        let Some(declared_idx) = columns
            .iter()
            .position(|column| column.name.eq_ignore_ascii_case(column_name))
        else {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "WITHOUT ROWID table `{table_name}` PRIMARY KEY references unknown column `{column_name}`"
                ),
            });
        };
        let collation = primary_key.key_collations[pk_position].clone();
        if primary_key_declared
            .iter()
            .zip(&primary_key_collations)
            .any(|(&existing_declared, existing_collation)| {
                existing_declared == declared_idx
                    && existing_collation
                        .as_deref()
                        .unwrap_or("BINARY")
                        .eq_ignore_ascii_case(collation.as_deref().unwrap_or("BINARY"))
            })
        {
            continue;
        }
        primary_key_declared.push(declared_idx);
        primary_key_sort_directions.push(primary_key.key_sort_directions[pk_position]);
        primary_key_collations.push(collation);
    }

    let storage_to_declared = primary_key_declared
        .iter()
        .copied()
        .chain((0..columns.len()).filter(|declared_idx| {
            !primary_key_declared
                .iter()
                .any(|primary_idx| primary_idx == declared_idx)
        }))
        .collect::<Vec<_>>();

    Ok(WithoutRowidStorageLayout {
        storage_to_declared,
        primary_key_declared,
        primary_key_sort_directions,
        primary_key_collations,
    })
}

#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
fn validate_builtin_storage_collation(
    table_name: &str,
    object_name: &str,
    collation: Option<&str>,
) -> Result<()> {
    if collation.is_none_or(|name| {
        name.eq_ignore_ascii_case("BINARY")
            || name.eq_ignore_ascii_case("NOCASE")
            || name.eq_ignore_ascii_case("RTRIM")
    }) {
        return Ok(());
    }
    Err(FrankenError::FunctionError(format!(
        "cannot persist WITHOUT ROWID table `{table_name}` object `{object_name}` with unsupported collation `{}`",
        collation.unwrap_or_default()
    )))
}

#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
fn without_rowid_secondary_index_layout(
    table: &TableSchema,
    index: &IndexSchema,
    _table_layout: &WithoutRowidStorageLayout,
    is_implicit_constraint_index: bool,
) -> Result<WithoutRowidSecondaryIndexLayout> {
    let shared = table
        .without_rowid_index_layout(index, is_implicit_constraint_index)
        .map_err(|error| FrankenError::DatabaseCorrupt {
            detail: format!(
                "WITHOUT ROWID index `{}.{}` has invalid physical layout: {error}",
                table.name, index.name
            ),
        })?;
    for collation in &shared.cursor_collations {
        validate_builtin_storage_collation(&table.name, &index.name, collation.as_deref())?;
    }

    Ok(WithoutRowidSecondaryIndexLayout {
        logical_declared: shared.logical_declared,
        locator_declared: shared.locator_declared,
        cursor_desc_flags: shared.cursor_desc_flags,
        cursor_collations: shared.cursor_collations,
    })
}

#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
fn prepare_without_rowid_persistence<S: BuildHasher>(
    schema: &[TableSchema],
    db: &MemDatabase,
    original_ddl: &HashMap<String, String, S>,
) -> Result<HashMap<String, PreparedWithoutRowidPersistence>> {
    let mut prepared = HashMap::new();
    for table in schema {
        let Some(mem_table) = db.get_table(table.root_page) else {
            continue;
        };
        let create_sql = original_ddl
            .get(&table.name.to_ascii_lowercase())
            .cloned()
            .unwrap_or_else(|| {
                build_create_table_sql_with_implicit_index_predicate(table, |index| {
                    parse_autoindex_ordinal(&index.name, &table.name).is_some()
                        && !original_ddl.contains_key(&index.name.to_ascii_lowercase())
                })
            });
        let Some(Statement::CreateTable(create)) = parse_single_statement(&create_sql) else {
            if table.without_rowid {
                return Err(FrankenError::FunctionError(format!(
                    "cannot persist WITHOUT ROWID table `{}` because its CREATE statement cannot be parsed",
                    table.name
                )));
            }
            continue;
        };
        if table.without_rowid != create.without_rowid {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "table `{}` WITHOUT ROWID schema flag disagrees with its CREATE statement",
                    table.name
                ),
            });
        }
        if !create.without_rowid {
            continue;
        }

        let implicit_specs = plan_implicit_constraint_indexes(&create, &table.name)?;
        let actual_implicit_index_names = table
            .indexes
            .iter()
            .filter(|index| {
                parse_autoindex_ordinal(&index.name, &table.name).is_some()
                    && !original_ddl.contains_key(&index.name.to_ascii_lowercase())
            })
            .map(|index| index.name.clone())
            .collect::<Vec<_>>();
        validate_auxiliary_implicit_index_inventory(
            &table.name,
            &implicit_specs,
            &actual_implicit_index_names,
        )?;

        if table
            .columns
            .iter()
            .any(|column| column.generated_expr.is_some())
        {
            return Err(FrankenError::FunctionError(format!(
                "cannot persist WITHOUT ROWID table `{}` with generated columns without a complete physical-column map",
                table.name
            )));
        }

        let table_layout =
            without_rowid_storage_layout(&table.name, &table.columns, &implicit_specs)?;
        let shared_table_layout =
            table
                .without_rowid_table_layout()
                .map_err(|error| FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "WITHOUT ROWID table `{}` has invalid in-memory physical layout: {error}",
                        table.name
                    ),
                })?;
        if shared_table_layout.storage_to_declared != table_layout.storage_to_declared
            || shared_table_layout.cursor_desc_flags != table_layout.table_cursor_desc_flags()
            || shared_table_layout.cursor_collations != table_layout.table_cursor_collations()
        {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "WITHOUT ROWID table `{}` in-memory physical metadata disagrees with its authoritative CREATE statement",
                    table.name
                ),
            });
        }
        for collation in &table_layout.primary_key_collations {
            validate_builtin_storage_collation(&table.name, &table.name, collation.as_deref())?;
        }
        for (_, values) in mem_table.iter_rows() {
            let _ = table_layout.storage_values_from_declared(&table.name, values)?;
        }
        if !mem_table.unique_column_group_is_valid(
            &table_layout.primary_key_declared,
            &table_layout.primary_key_collations,
        ) {
            let columns = table_layout
                .primary_key_declared
                .iter()
                .map(|&declared_idx| table.columns[declared_idx].name.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            return Err(FrankenError::UniqueViolation {
                columns: format!("{}.{}", table.name, columns),
            });
        }

        let col_map = table
            .columns
            .iter()
            .map(|column| (table.name.clone(), column.name.clone(), false))
            .collect::<Vec<_>>();
        let mut secondary_layouts = Vec::with_capacity(table.indexes.len());
        for index in &table.indexes {
            let is_implicit_constraint_index = actual_implicit_index_names
                .iter()
                .any(|name| name.eq_ignore_ascii_case(&index.name));
            let layout = without_rowid_secondary_index_layout(
                table,
                index,
                &table_layout,
                is_implicit_constraint_index,
            )?;
            if index.is_unique
                && index.where_clause.is_none()
                && !mem_table.unique_column_group_is_valid(
                    &layout.logical_declared,
                    &layout.cursor_collations[..layout.logical_declared.len()],
                )
            {
                return Err(FrankenError::UniqueViolation {
                    columns: format!("{}.{}", table.name, index.key_label()),
                });
            }
            if let Some(predicate_sql) = index.where_clause.as_deref() {
                let predicate =
                    fsqlite_parser::expr::parse_expr(predicate_sql).map_err(|error| {
                        FrankenError::FunctionError(format!(
                            "cannot persist WITHOUT ROWID table `{}` partial index `{}` because its predicate cannot be parsed: {error}",
                            table.name, index.name
                        ))
                    })?;
                let mut matching_rows = MemDatabase::new();
                let matching_root = matching_rows.create_table(table.columns.len());
                for (rowid, values) in mem_table.iter_rows() {
                    let predicate_value =
                        eval_join_expr(&predicate, values, &col_map).map_err(|error| {
                            FrankenError::FunctionError(format!(
                                "cannot persist WITHOUT ROWID table `{}` partial index `{}` because its predicate cannot be evaluated: {error}",
                                table.name, index.name
                            ))
                        })?;
                    if is_sqlite_truthy(&predicate_value) {
                        matching_rows
                            .get_table_mut(matching_root)
                            .ok_or_else(|| {
                                FrankenError::Internal(format!(
                                    "WITHOUT ROWID partial-index preflight lost scratch table for `{}`",
                                    index.name
                                ))
                            })?
                            .insert_row(rowid, values.to_vec());
                    }
                }
                if index.is_unique
                    && !matching_rows
                        .get_table(matching_root)
                        .ok_or_else(|| {
                            FrankenError::Internal(format!(
                                "WITHOUT ROWID partial-index preflight lost scratch table for `{}`",
                                index.name
                            ))
                        })?
                        .unique_column_group_is_valid(
                            &layout.logical_declared,
                            &layout.cursor_collations[..layout.logical_declared.len()],
                        )
                {
                    return Err(FrankenError::UniqueViolation {
                        columns: format!("{}.{}", table.name, index.key_label()),
                    });
                }
            }
            secondary_layouts.push(layout);
        }

        if prepared
            .insert(
                table.name.to_ascii_lowercase(),
                PreparedWithoutRowidPersistence {
                    table_layout,
                    secondary_layouts,
                },
            )
            .is_some()
        {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "duplicate WITHOUT ROWID table name in persistence schema: `{}`",
                    table.name
                ),
            });
        }
    }
    Ok(prepared)
}

pub(crate) fn extract_foreign_keys_from_sql(sql: &str, columns: &[ColumnInfo]) -> Vec<FkDef> {
    let Some(Statement::CreateTable(create)) = parse_single_statement(sql) else {
        return extract_foreign_keys_sql_fallback(sql, columns);
    };
    let CreateTableBody::Columns {
        columns: column_defs,
        constraints,
    } = &create.body
    else {
        return Vec::new();
    };

    let mut foreign_keys = Vec::new();
    for (column_index, column) in column_defs.iter().enumerate() {
        for constraint in &column.constraints {
            if let ColumnConstraintKind::ForeignKey(clause) = &constraint.kind {
                foreign_keys.push(fk_clause_to_def(
                    &[column_index],
                    Some(column.name.clone()),
                    clause,
                ));
            }
        }
    }
    for constraint in constraints {
        if let TableConstraintKind::ForeignKey {
            columns: child_columns,
            clause,
        } = &constraint.kind
        {
            let child_indices = child_columns
                .iter()
                .filter_map(|column_name| {
                    columns
                        .iter()
                        .position(|column| column.name.eq_ignore_ascii_case(column_name))
                })
                .collect::<Vec<_>>();
            if !child_indices.is_empty() {
                foreign_keys.push(fk_clause_to_def(&child_indices, None, clause));
            }
        }
    }

    foreign_keys
}

fn extract_foreign_keys_sql_fallback(sql: &str, columns: &[ColumnInfo]) -> Vec<FkDef> {
    let Some(open) = find_unquoted_sql_char(sql, '(') else {
        return Vec::new();
    };
    let Some(close) = find_matching_sql_paren(sql, open) else {
        return Vec::new();
    };
    let mut foreign_keys = Vec::new();

    for definition in split_top_level_csv_items(&sql[open + 1..close]) {
        if starts_with_unquoted_table_constraint(&definition) {
            let Some(foreign_pos) = find_top_level_unquoted_sql_keyword(&definition, "FOREIGN")
            else {
                continue;
            };
            let after_foreign = &definition[foreign_pos + "FOREIGN".len()..];
            let Some(key_pos) = find_top_level_unquoted_sql_keyword(after_foreign, "KEY") else {
                continue;
            };
            let after_key = &after_foreign[key_pos + "KEY".len()..];
            let child_list = trim_leading_sql_space_and_comments(after_key);
            if !child_list.starts_with('(') {
                continue;
            }
            let open_paren = definition.len() - child_list.len();
            let Some(close_paren) = find_matching_sql_paren(&definition, open_paren) else {
                continue;
            };
            let Some(child_names) =
                parse_sql_identifier_list(&definition[open_paren + 1..close_paren])
            else {
                continue;
            };
            let Some(child_indices) = child_names
                .iter()
                .map(|name| {
                    columns
                        .iter()
                        .position(|column| column.name.eq_ignore_ascii_case(name))
                })
                .collect::<Option<Vec<_>>>()
            else {
                continue;
            };
            if let Some(fk) =
                parse_fk_reference_sql(&definition[close_paren + 1..], &child_indices, None)
            {
                foreign_keys.push(fk);
            }
            continue;
        }

        let Some((column_name, remainder)) = parse_column_name_and_remainder(&definition) else {
            continue;
        };
        let Some(column_index) = columns
            .iter()
            .position(|column| column.name.eq_ignore_ascii_case(&column_name))
        else {
            continue;
        };
        if let Some(fk) = parse_fk_reference_sql(remainder, &[column_index], Some(column_name)) {
            foreign_keys.push(fk);
        }
    }

    foreign_keys
}

fn parse_sql_identifier_list(input: &str) -> Option<Vec<String>> {
    split_top_level_csv_items(input)
        .into_iter()
        .map(|item| {
            let (name, remainder) = parse_column_name_and_remainder(&item)?;
            trim_leading_sql_space_and_comments(remainder)
                .is_empty()
                .then_some(name)
        })
        .collect()
}

fn parse_fk_reference_sql(
    input: &str,
    child_indices: &[usize],
    owner_column: Option<String>,
) -> Option<FkDef> {
    let references_pos = find_top_level_unquoted_sql_keyword(input, "REFERENCES")?;
    let after_references =
        trim_leading_sql_space_and_comments(&input[references_pos + "REFERENCES".len()..]);
    let (parent_table, after_parent_table) = parse_fk_parent_table(after_references)?;
    let after_parent_table = trim_leading_sql_space_and_comments(after_parent_table);
    let (parent_columns, action_sql) = if after_parent_table.starts_with('(') {
        let open_paren = input.len() - after_parent_table.len();
        let close_paren = find_matching_sql_paren(input, open_paren)?;
        (
            parse_sql_identifier_list(&input[open_paren + 1..close_paren])?,
            &input[close_paren + 1..],
        )
    } else {
        (Vec::new(), after_parent_table)
    };
    let tokens = collect_unquoted_sql_keyword_tokens(action_sql)
        .into_iter()
        .map(|(token, _)| token)
        .collect::<Vec<_>>();
    let mut on_delete = FkActionType::NoAction;
    let mut on_update = FkActionType::NoAction;
    for (index, token) in tokens.iter().enumerate() {
        if token != "ON" || index + 2 >= tokens.len() {
            continue;
        }
        let action = match tokens[index + 2].as_str() {
            "CASCADE" => Some(FkActionType::Cascade),
            "RESTRICT" => Some(FkActionType::Restrict),
            "SET" if tokens.get(index + 3).is_some_and(|token| token == "NULL") => {
                Some(FkActionType::SetNull)
            }
            "SET"
                if tokens
                    .get(index + 3)
                    .is_some_and(|token| token == "DEFAULT") =>
            {
                Some(FkActionType::SetDefault)
            }
            "NO" if tokens.get(index + 3).is_some_and(|token| token == "ACTION") => {
                Some(FkActionType::NoAction)
            }
            _ => None,
        };
        match (tokens[index + 1].as_str(), action) {
            ("DELETE", Some(action)) => on_delete = action,
            ("UPDATE", Some(action)) => on_update = action,
            _ => {}
        }
    }
    let deferred = tokens.windows(3).enumerate().any(|(index, window)| {
        (index == 0 || tokens[index - 1] != "NOT")
            && window[0] == "DEFERRABLE"
            && window[1] == "INITIALLY"
            && window[2] == "DEFERRED"
    });

    Some(FkDef {
        child_columns: child_indices.to_vec(),
        owner_column,
        parent_table,
        parent_columns,
        on_delete,
        on_update,
        deferred,
    })
}

fn parse_fk_parent_table(input: &str) -> Option<(String, &str)> {
    let trimmed = trim_leading_sql_space_and_comments(input);
    if trimmed.is_empty() {
        return None;
    }
    let (name_raw, remainder) = match trimmed.as_bytes()[0] {
        b'"' => parse_quoted_identifier(trimmed, b'"', b'"')?,
        b'`' => parse_quoted_identifier(trimmed, b'`', b'`')?,
        b'[' => parse_bracket_identifier(trimmed)?,
        _ => {
            let mut chars = trimmed.char_indices().peekable();
            let mut end = trimmed.len();
            while let Some((index, ch)) = chars.next() {
                let starts_comment = matches!(ch, '-' | '/')
                    && chars.peek().is_some_and(|(_, next)| {
                        (ch == '-' && *next == '-') || (ch == '/' && *next == '*')
                    });
                if ch.is_whitespace() || ch == '(' || starts_comment {
                    end = index;
                    break;
                }
            }
            (&trimmed[..end], &trimmed[end..])
        }
    };
    (!name_raw.is_empty()).then(|| {
        (
            strip_identifier_quotes(name_raw),
            trim_leading_sql_space_and_comments(remainder),
        )
    })
}

fn fk_clause_to_def(
    child_indices: &[usize],
    owner_column: Option<String>,
    clause: &fsqlite_ast::ForeignKeyClause,
) -> FkDef {
    let mut on_delete = FkActionType::NoAction;
    let mut on_update = FkActionType::NoAction;
    for action in &clause.actions {
        let action_type = match action.action {
            fsqlite_ast::ForeignKeyActionType::SetNull => FkActionType::SetNull,
            fsqlite_ast::ForeignKeyActionType::SetDefault => FkActionType::SetDefault,
            fsqlite_ast::ForeignKeyActionType::Cascade => FkActionType::Cascade,
            fsqlite_ast::ForeignKeyActionType::Restrict => FkActionType::Restrict,
            fsqlite_ast::ForeignKeyActionType::NoAction => FkActionType::NoAction,
        };
        match action.trigger {
            fsqlite_ast::ForeignKeyTrigger::OnDelete => on_delete = action_type,
            fsqlite_ast::ForeignKeyTrigger::OnUpdate => on_update = action_type,
        }
    }
    let deferred = clause.deferrable.as_ref().is_some_and(|d| {
        !d.not
            && matches!(
                d.initially,
                Some(fsqlite_ast::DeferrableInitially::Deferred)
            )
    });
    FkDef {
        child_columns: child_indices.to_vec(),
        owner_column,
        parent_table: clause.table.clone(),
        parent_columns: clause.columns.clone(),
        on_delete,
        on_update,
        deferred,
    }
}

/// Indexed term metadata used to reconstruct `CREATE INDEX` SQL.
#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
pub(crate) struct CreateIndexSqlTerm<'a> {
    pub(crate) column_name: &'a str,
    pub(crate) collation: Option<&'a str>,
    pub(crate) direction: Option<SortDirection>,
}

/// Reconstruct a `CREATE INDEX` statement from index metadata.
/// Needed for sqlite_master row generation during schema persistence — not
/// yet wired into the live schema write-back path.
#[allow(dead_code)]
pub(crate) fn build_create_index_sql(
    index_name: &str,
    table_name: &str,
    unique: bool,
    terms: &[CreateIndexSqlTerm<'_>],
    where_clause: Option<&fsqlite_ast::Expr>,
) -> String {
    use std::fmt::Write as _;
    let mut sql = if unique {
        format!(
            "CREATE UNIQUE INDEX {} ON {} (",
            quote_identifier(index_name),
            quote_identifier(table_name)
        )
    } else {
        format!(
            "CREATE INDEX {} ON {} (",
            quote_identifier(index_name),
            quote_identifier(table_name)
        )
    };
    for (i, term) in terms.iter().enumerate() {
        if i > 0 {
            sql.push_str(", ");
        }
        sql.push_str(&quote_identifier(term.column_name));
        if let Some(collation) = term.collation {
            let _ = write!(sql, " COLLATE {}", quote_identifier(collation));
        }
        match term.direction {
            Some(SortDirection::Asc) => sql.push_str(" ASC"),
            Some(SortDirection::Desc) => sql.push_str(" DESC"),
            None => {}
        }
    }
    sql.push(')');
    if let Some(expr) = where_clause {
        let _ = write!(sql, " WHERE {expr}");
    }
    sql
}

fn build_create_expression_index_sql(
    index_name: &str,
    table_name: &str,
    unique: bool,
    expressions: &[String],
    collations: &[Option<String>],
    directions: &[SortDirection],
    where_clause: Option<&str>,
) -> String {
    use std::fmt::Write as _;
    let mut sql = if unique {
        format!(
            "CREATE UNIQUE INDEX {} ON {} (",
            quote_identifier(index_name),
            quote_identifier(table_name)
        )
    } else {
        format!(
            "CREATE INDEX {} ON {} (",
            quote_identifier(index_name),
            quote_identifier(table_name)
        )
    };
    for (i, expr) in expressions.iter().enumerate() {
        if i > 0 {
            sql.push_str(", ");
        }
        sql.push_str(expr);
        let expression_already_declares_collation = unquoted_sql_keyword_tokens(expr)
            .iter()
            .any(|token| token == "COLLATE");
        if !expression_already_declares_collation
            && let Some(collation) = collations.get(i).and_then(|c| c.as_deref())
        {
            let _ = write!(sql, " COLLATE {}", quote_identifier(collation));
        }
        match directions.get(i).copied() {
            Some(SortDirection::Asc) => sql.push_str(" ASC"),
            Some(SortDirection::Desc) => sql.push_str(" DESC"),
            None => {}
        }
    }
    sql.push(')');
    if let Some(predicate) = where_clause {
        let _ = write!(sql, " WHERE {predicate}");
    }
    sql
}

/// Parse column info from a CREATE TABLE SQL string.
///
/// This is a best-effort parser that handles the common case of
/// `CREATE TABLE "name" ("col1" TYPE, "col2" TYPE, ...)`.
/// Extracts column names and affinities from the column definitions.
/// Used by `load_from_sqlite` and `reload_memdb_from_pager` (bd-1ene).
pub fn parse_columns_from_create_sql(sql: &str) -> Vec<ColumnInfo> {
    if let Some(columns) = try_parse_columns_from_create_sql_ast(sql) {
        return columns;
    }

    let is_strict = is_strict_table_sql(sql);
    let is_without_rowid = is_without_rowid_table_sql(sql);
    // Find the parenthesized column list.
    let Some(open) = find_unquoted_sql_char(sql, '(') else {
        return Vec::new();
    };
    let Some(close) = find_matching_sql_paren(sql, open) else {
        return Vec::new();
    };

    let body = &sql[open + 1..close];
    split_top_level_csv_items(body)
        .into_iter()
        .filter_map(|col_def| {
            if starts_with_unquoted_table_constraint(&col_def) {
                return None;
            }

            let (name, remainder) = parse_column_name_and_remainder(&col_def)?;
            let tokens: Vec<&str> = remainder.split_whitespace().collect();
            let type_decl = extract_type_declaration(&tokens);
            let affinity = type_to_affinity(&type_decl);
            let keyword_tokens = unquoted_sql_keyword_tokens(remainder);
            let has_primary_key =
                unquoted_tokens_contain_phrase(&keyword_tokens, &["PRIMARY", "KEY"]);
            let has_primary_key_desc =
                unquoted_tokens_contain_phrase(&keyword_tokens, &["PRIMARY", "KEY", "DESC"]);
            let has_unique = keyword_tokens
                .iter()
                .any(|keyword| matches!(keyword.as_str(), "UNIQUE"));
            let has_not_null = unquoted_tokens_contain_phrase(&keyword_tokens, &["NOT", "NULL"]);
            let is_ipk = !is_without_rowid
                && has_primary_key
                && !has_primary_key_desc
                && type_decl.eq_ignore_ascii_case("INTEGER");
            let type_name = if type_decl.is_empty() {
                None
            } else {
                Some(type_decl)
            };
            let strict_type = if is_strict {
                type_name
                    .as_deref()
                    .and_then(StrictColumnType::from_type_name)
            } else {
                None
            };

            let default_value = extract_default_value(remainder);

            let collation = extract_collation_name(remainder);
            let (generated_expr, generated_stored) = extract_generated_column_clause(remainder);

            Some(ColumnInfo {
                name,
                affinity,
                is_ipk,
                type_name,
                notnull: has_not_null,
                unique: has_unique || has_primary_key,
                default_value,
                strict_type,
                generated_expr,
                generated_stored,
                collation,
                conflict_action: None,
            })
        })
        .collect()
}

/// Extract column metadata from sqlite_master SQL for both ordinary and
/// materialized virtual tables.
#[must_use]
pub fn parse_columns_from_sqlite_master_sql(sql: &str) -> Vec<ColumnInfo> {
    if is_virtual_table_sql(sql) {
        return parse_virtual_table_columns_from_sql(sql)
            .unwrap_or_else(|| parse_columns_from_create_sql(sql));
    }
    parse_columns_from_create_sql(sql)
}

pub(crate) fn validate_sqlite_master_root_page(name: &str, root_page_num: i64) -> Result<u32> {
    if root_page_num <= 0 {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!("sqlite_master entry `{name}` has invalid rootpage {root_page_num}"),
        });
    }

    let root_page_u32 =
        u32::try_from(root_page_num).map_err(|_| FrankenError::DatabaseCorrupt {
            detail: format!(
                "sqlite_master entry `{name}` has out-of-range rootpage {root_page_num}"
            ),
        })?;
    i32::try_from(root_page_u32).map_err(|_| FrankenError::DatabaseCorrupt {
        detail: format!(
            "sqlite_master entry `{name}` has rootpage {root_page_num} that exceeds supported range"
        ),
    })?;
    Ok(root_page_u32)
}

fn is_virtual_table_sql(sql: &str) -> bool {
    sql.trim_start()
        .to_ascii_uppercase()
        .starts_with("CREATE VIRTUAL TABLE")
}

#[must_use]
pub fn is_without_rowid_table_sql(sql: &str) -> bool {
    if let Some(Statement::CreateTable(create)) = parse_single_statement(sql) {
        return create.without_rowid;
    }

    let Some(close_paren) = sql.rfind(')') else {
        return false;
    };
    let tail = &sql[close_paren + 1..];
    unquoted_tokens_contain_phrase(&unquoted_sql_keyword_tokens(tail), &["WITHOUT", "ROWID"])
}

fn parse_virtual_table_columns_from_sql(sql: &str) -> Option<Vec<ColumnInfo>> {
    let mut parser = Parser::from_sql(sql);
    let (statements, errors) = parser.parse_all();
    if !errors.is_empty() || statements.len() != 1 {
        return None;
    }
    match statements.into_iter().next()? {
        Statement::CreateVirtualTable(create) => {
            Some(parse_virtual_table_column_infos(&create.args))
        }
        _ => None,
    }
}

fn parse_virtual_table_column_infos(args: &[String]) -> Vec<ColumnInfo> {
    let mut columns = Vec::new();
    let mut seen = std::collections::HashSet::<String>::new();

    for arg in args {
        let trimmed = arg.trim();
        if trimmed.is_empty() || trimmed.contains('=') {
            continue;
        }
        let raw_name = trimmed
            .split_whitespace()
            .next()
            .unwrap_or_default()
            .trim_matches(|ch| matches!(ch, '"' | '\'' | '`' | '[' | ']'));
        if raw_name.is_empty() {
            continue;
        }
        let key = raw_name.to_ascii_lowercase();
        if !seen.insert(key) {
            continue;
        }
        columns.push(ColumnInfo {
            name: raw_name.to_owned(),
            affinity: 'C',
            is_ipk: false,
            type_name: None,
            notnull: false,
            unique: false,
            default_value: None,
            strict_type: None,
            generated_expr: None,
            generated_stored: None,
            collation: None,
            conflict_action: None,
        });
    }

    if columns.is_empty() {
        columns.push(ColumnInfo {
            name: "content".to_owned(),
            affinity: 'C',
            is_ipk: false,
            type_name: None,
            notnull: false,
            unique: false,
            default_value: None,
            strict_type: None,
            generated_expr: None,
            generated_stored: None,
            collation: None,
            conflict_action: None,
        });
    }

    columns
}

/// Return true when CREATE TABLE SQL declares the table as STRICT.
#[must_use]
pub fn is_strict_table_sql(sql: &str) -> bool {
    if let Some(Statement::CreateTable(create)) = parse_single_statement(sql) {
        return create.strict;
    }

    let Some(close_paren) = sql.rfind(')') else {
        return false;
    };
    let tail = &sql[close_paren + 1..];
    unquoted_sql_keyword_tokens(tail)
        .iter()
        .any(|keyword| matches!(keyword.as_str(), "STRICT"))
}

/// Return true when CREATE TABLE SQL declares AUTOINCREMENT.
#[must_use]
pub fn is_autoincrement_table_sql(sql: &str) -> bool {
    if let Some(Statement::CreateTable(create)) = parse_single_statement(sql) {
        return autoincrement_from_create_table_statement(&create);
    }

    unquoted_sql_keyword_tokens(sql)
        .iter()
        .any(|keyword| matches!(keyword.as_str(), "AUTOINCREMENT"))
}

pub(crate) fn autoincrement_from_create_table_statement(create: &CreateTableStatement) -> bool {
    let CreateTableBody::Columns { columns, .. } = &create.body else {
        return false;
    };
    columns.iter().any(|col| {
        let is_integer = col
            .type_name
            .as_ref()
            .is_some_and(|tn| tn.name.eq_ignore_ascii_case("INTEGER"));
        is_integer
            && col.constraints.iter().any(|constraint| {
                matches!(
                    &constraint.kind,
                    ColumnConstraintKind::PrimaryKey {
                        autoincrement: true,
                        direction,
                        ..
                    } if *direction != Some(SortDirection::Desc)
                )
            })
    })
}

/// Extract CHECK constraint expressions from a CREATE TABLE SQL string.
///
/// Finds `CHECK(...)` clauses in the column-def body and returns the
/// expression text (inside the parentheses) for each one.
#[must_use]
pub fn extract_check_constraints_from_sql(sql: &str) -> Vec<String> {
    extract_check_constraints_with_owners_from_sql(sql)
        .into_iter()
        .map(|check| check.expr)
        .collect()
}

pub(crate) fn extract_check_constraints_with_owners_from_sql(sql: &str) -> Vec<CheckConstraint> {
    if let Some(Statement::CreateTable(create)) = parse_single_statement(sql) {
        return check_constraints_from_create_table_statement(&create);
    }

    extract_check_constraints_with_owners_sql_fallback(sql)
}

fn extract_check_constraints_with_owners_sql_fallback(sql: &str) -> Vec<CheckConstraint> {
    let Some(open) = find_unquoted_sql_char(sql, '(') else {
        return Vec::new();
    };
    let Some(close) = find_matching_sql_paren(sql, open) else {
        return Vec::new();
    };
    let body = &sql[open + 1..close];
    let mut checks = Vec::new();

    for definition in split_top_level_csv_items(body) {
        let owner_column = if starts_with_unquoted_table_constraint(&definition) {
            None
        } else {
            parse_column_name_and_remainder(&definition).map(|(name, _)| name)
        };
        let mut search_from = 0_usize;
        while let Some(relative_check) =
            find_unquoted_sql_keyword(&definition[search_from..], "CHECK")
        {
            let check_start = search_from + relative_check;
            let after_keyword = &definition[check_start + "CHECK".len()..];
            let after_space_and_comments = trim_leading_sql_space_and_comments(after_keyword);
            let skipped = after_keyword.len() - after_space_and_comments.len();
            let open_paren = check_start + "CHECK".len() + skipped;
            if !after_space_and_comments.starts_with('(') {
                search_from = check_start + "CHECK".len();
                continue;
            }
            let Some(close_paren) = find_matching_sql_paren(&definition, open_paren) else {
                break;
            };
            checks.push(CheckConstraint {
                expr: definition[open_paren + 1..close_paren].trim().to_owned(),
                owner_column: owner_column.clone(),
            });
            search_from = close_paren + 1;
        }
    }
    checks
}

pub(crate) fn check_constraints_from_create_table_statement(
    create: &CreateTableStatement,
) -> Vec<CheckConstraint> {
    let CreateTableBody::Columns {
        columns,
        constraints,
    } = &create.body
    else {
        return Vec::new();
    };
    let mut checks = Vec::new();
    for column in columns {
        for constraint in &column.constraints {
            if let ColumnConstraintKind::Check(expr) = &constraint.kind {
                checks.push(CheckConstraint {
                    expr: expr.to_string(),
                    owner_column: Some(column.name.clone()),
                });
            }
        }
    }
    for constraint in constraints {
        if let TableConstraintKind::Check { expr, .. } = &constraint.kind {
            checks.push(CheckConstraint {
                expr: expr.to_string(),
                owner_column: None,
            });
        }
    }
    checks
}

fn parse_column_name_and_remainder(def: &str) -> Option<(String, &str)> {
    let trimmed = def.trim_start();
    if trimmed.is_empty() {
        return None;
    }
    let bytes = trimmed.as_bytes();
    let (name_raw, remainder) = match bytes[0] {
        b'"' => parse_quoted_identifier(trimmed, b'"', b'"')?,
        b'`' => parse_quoted_identifier(trimmed, b'`', b'`')?,
        b'[' => parse_bracket_identifier(trimmed)?,
        _ => {
            let end = find_unquoted_name_end(trimmed);
            (&trimmed[..end], &trimmed[end..])
        }
    };
    Some((
        strip_identifier_quotes(name_raw),
        trim_leading_sql_space_and_comments(remainder),
    ))
}

fn parse_single_statement(sql: &str) -> Option<Statement> {
    let mut parser = Parser::from_sql(sql);
    let (statements, errors) = parser.parse_all();
    if !errors.is_empty() || statements.len() != 1 {
        return None;
    }
    statements.into_iter().next()
}

fn format_default_value(dv: &DefaultValue) -> String {
    match dv {
        DefaultValue::Expr(expr) => expr.to_string(),
        DefaultValue::ParenExpr(expr) => format!("({expr})"),
    }
}

fn indexed_column_name(indexed_column: &IndexedColumn) -> Option<&str> {
    fn extract(expr: &Expr) -> Option<&str> {
        match expr {
            Expr::Column(col_ref, _) if col_ref.table.is_none() => Some(&col_ref.column),
            Expr::Collate { expr, .. } => extract(expr),
            _ => None,
        }
    }

    extract(&indexed_column.expr)
}

fn indexed_column_collation(indexed_column: &IndexedColumn) -> Option<String> {
    fn extract(expr: &Expr) -> Option<&str> {
        match expr {
            Expr::Collate {
                expr, collation, ..
            } => extract(expr).or(Some(collation.as_str())),
            _ => None,
        }
    }

    indexed_column
        .collation
        .clone()
        .or_else(|| extract(&indexed_column.expr).map(str::to_owned))
}

fn strip_wrapping_default_parens(mut default_sql: &str) -> &str {
    loop {
        let trimmed = default_sql.trim();
        let bytes = trimmed.as_bytes();
        if bytes.first() != Some(&b'(') || bytes.last() != Some(&b')') {
            return trimmed;
        }

        let mut depth = 0_i32;
        let mut idx = 0_usize;
        let mut wraps_entire_expr = false;
        while idx < bytes.len() {
            match bytes[idx] {
                quote @ (b'\'' | b'"') => {
                    idx += 1;
                    while idx < bytes.len() {
                        if bytes[idx] == quote {
                            if idx + 1 < bytes.len() && bytes[idx + 1] == quote {
                                idx += 2;
                            } else {
                                idx += 1;
                                break;
                            }
                        } else {
                            idx += 1;
                        }
                    }
                    continue;
                }
                b'(' => depth += 1,
                b')' => {
                    depth -= 1;
                    if depth == 0 {
                        wraps_entire_expr = idx == bytes.len() - 1;
                        break;
                    }
                    if depth < 0 {
                        return trimmed;
                    }
                }
                _ => {}
            }
            idx += 1;
        }

        if !wraps_entire_expr || depth != 0 {
            return trimmed;
        }
        default_sql = &trimmed[1..trimmed.len() - 1];
    }
}

fn parse_wrapped_default_text(default_sql: &str, quote: char) -> Option<SqliteValue> {
    if !default_sql.starts_with(quote) {
        return None;
    }
    let mut value = String::new();
    let body = &default_sql[quote.len_utf8()..];
    let mut chars = body.char_indices().peekable();

    while let Some((offset, ch)) = chars.next() {
        if ch != quote {
            value.push(ch);
            continue;
        }
        if let Some((_, next_ch)) = chars.peek()
            && *next_ch == quote
        {
            value.push(quote);
            let _ = chars.next();
            continue;
        }
        let absolute_end = quote.len_utf8() + offset + ch.len_utf8();
        return (absolute_end == default_sql.len()).then(|| SqliteValue::Text(value.into()));
    }

    None
}

fn loaded_default_literal_value(literal: &Literal) -> Option<SqliteValue> {
    match literal {
        Literal::Integer(value) => Some(SqliteValue::Integer(*value)),
        Literal::Float(value) => Some(SqliteValue::Float(*value)),
        Literal::String(value) => Some(SqliteValue::Text(value.clone().into())),
        Literal::Blob(value) => Some(SqliteValue::from(value.clone())),
        Literal::Null => Some(SqliteValue::Null),
        Literal::True => Some(SqliteValue::Integer(1)),
        Literal::False => Some(SqliteValue::Integer(0)),
        Literal::CurrentTime | Literal::CurrentDate | Literal::CurrentTimestamp => None,
    }
}

fn loaded_constant_default_expr_value(expr: &Expr) -> Option<SqliteValue> {
    match expr {
        Expr::Literal(literal, _) => loaded_default_literal_value(literal),
        Expr::UnaryOp {
            op: UnaryOp::Plus,
            expr,
            ..
        } => match loaded_constant_default_expr_value(expr)? {
            value @ (SqliteValue::Integer(_) | SqliteValue::Float(_)) => Some(value),
            _ => None,
        },
        Expr::UnaryOp {
            op: UnaryOp::Negate,
            expr,
            ..
        } => match loaded_constant_default_expr_value(expr)? {
            SqliteValue::Integer(value) => Some(
                value
                    .checked_neg()
                    .map_or_else(|| SqliteValue::Float(-(value as f64)), SqliteValue::Integer),
            ),
            SqliteValue::Float(value) => Some(SqliteValue::Float(-value)),
            _ => None,
        },
        _ => None,
    }
}

fn parse_loaded_column_default_value(default_sql: &str) -> SqliteValue {
    let default_sql = strip_wrapping_default_parens(default_sql);
    if let Some(value) = parse_wrapped_default_text(default_sql, '\'')
        .or_else(|| parse_wrapped_default_text(default_sql, '"'))
    {
        return value;
    }
    if let Ok(expr) = fsqlite_parser::expr::parse_expr(default_sql)
        && let Some(value) = loaded_constant_default_expr_value(&expr)
    {
        return value;
    }
    SqliteValue::Text(default_sql.into())
}

fn inflate_loaded_table_row_values(
    values: &mut Vec<SqliteValue>,
    rowid: i64,
    columns: &[ColumnInfo],
    rowid_alias_col_idx: Option<usize>,
    table_name: &str,
) -> Result<()> {
    let num_columns = columns.len();
    if values.len() > num_columns {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "table `{table_name}` rowid {rowid} payload has {} columns; expected at most {num_columns}",
                values.len()
            ),
        });
    }
    if let Some(ipk_idx) = rowid_alias_col_idx
        && ipk_idx >= num_columns
    {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "table `{table_name}` rowid {rowid} has invalid INTEGER PRIMARY KEY alias column index {ipk_idx}"
            ),
        });
    }

    let payload_values = std::mem::take(values);
    let inflated = inflate_loaded_table_row_values_from_payload(
        &payload_values,
        rowid,
        columns,
        rowid_alias_col_idx,
        table_name,
    )?;
    *values = inflated;

    Ok(())
}

fn inflate_loaded_table_row_values_from_payload(
    payload_values: &[SqliteValue],
    rowid: i64,
    columns: &[ColumnInfo],
    rowid_alias_col_idx: Option<usize>,
    table_name: &str,
) -> Result<Vec<SqliteValue>> {
    let Some(ipk_idx) = rowid_alias_col_idx else {
        return inflate_loaded_table_row_values_with_alias_alignment(
            payload_values,
            rowid,
            columns,
            None,
            false,
            table_name,
        );
    };

    if payload_values.len() == columns.len() {
        return inflate_loaded_table_row_values_with_alias_alignment(
            payload_values,
            rowid,
            columns,
            Some(ipk_idx),
            true,
            table_name,
        );
    }

    let Some(value_at_alias_position) = payload_values.get(ipk_idx) else {
        return inflate_loaded_table_row_values_with_alias_alignment(
            payload_values,
            rowid,
            columns,
            Some(ipk_idx),
            false,
            table_name,
        );
    };

    let alias_slot_could_be_present = match value_at_alias_position {
        SqliteValue::Null => true,
        SqliteValue::Integer(encoded_rowid) => *encoded_rowid == rowid,
        _ => false,
    };
    if !alias_slot_could_be_present {
        return inflate_loaded_table_row_values_with_alias_alignment(
            payload_values,
            rowid,
            columns,
            Some(ipk_idx),
            false,
            table_name,
        );
    }

    let with_alias = inflate_loaded_table_row_values_with_alias_alignment(
        payload_values,
        rowid,
        columns,
        Some(ipk_idx),
        true,
        table_name,
    )?;
    let without_alias = inflate_loaded_table_row_values_with_alias_alignment(
        payload_values,
        rowid,
        columns,
        Some(ipk_idx),
        false,
        table_name,
    )?;
    let with_alias_valid = loaded_row_values_satisfy_notnull(columns, &with_alias);
    let without_alias_valid = loaded_row_values_satisfy_notnull(columns, &without_alias);

    if !with_alias_valid && !without_alias_valid {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "table `{table_name}` rowid {rowid} short payload violates NOT NULL constraints under both rowid-alias alignments"
            ),
        });
    }
    if with_alias_valid
        && (!without_alias_valid || matches!(value_at_alias_position, SqliteValue::Null))
    {
        Ok(with_alias)
    } else {
        Ok(without_alias)
    }
}

fn inflate_loaded_table_row_values_with_alias_alignment(
    payload_values: &[SqliteValue],
    rowid: i64,
    columns: &[ColumnInfo],
    rowid_alias_col_idx: Option<usize>,
    payload_includes_rowid_alias: bool,
    table_name: &str,
) -> Result<Vec<SqliteValue>> {
    let mut inflated = Vec::with_capacity(columns.len());
    let mut payload_idx = 0_usize;

    for (col_idx, column) in columns.iter().enumerate() {
        if rowid_alias_col_idx == Some(col_idx) && !payload_includes_rowid_alias {
            inflated.push(SqliteValue::Integer(rowid));
            continue;
        }

        let value = if let Some(value) = payload_values.get(payload_idx) {
            payload_idx += 1;
            value.clone()
        } else if let Some(default_sql) = column.default_value.as_ref() {
            parse_loaded_column_default_value(default_sql)
        } else {
            SqliteValue::Null
        };

        if rowid_alias_col_idx == Some(col_idx) {
            match &value {
                SqliteValue::Null => {
                    inflated.push(SqliteValue::Integer(rowid));
                    continue;
                }
                SqliteValue::Integer(encoded_rowid) if *encoded_rowid == rowid => {}
                SqliteValue::Integer(encoded_rowid) => {
                    return Err(FrankenError::DatabaseCorrupt {
                        detail: format!(
                            "table `{table_name}` rowid {rowid} stores inconsistent INTEGER PRIMARY KEY alias value {encoded_rowid}"
                        ),
                    });
                }
                other => {
                    return Err(FrankenError::DatabaseCorrupt {
                        detail: format!(
                            "table `{table_name}` rowid {rowid} stores non-integer INTEGER PRIMARY KEY alias value {other:?}"
                        ),
                    });
                }
            }
        }

        inflated.push(value);
    }

    if payload_idx != payload_values.len() {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "table `{table_name}` rowid {rowid} left {} payload columns unconsumed after rowid-alias inflation",
                payload_values.len() - payload_idx
            ),
        });
    }

    Ok(inflated)
}

fn loaded_row_values_satisfy_notnull(columns: &[ColumnInfo], values: &[SqliteValue]) -> bool {
    values.len() == columns.len()
        && columns.iter().zip(values.iter()).all(|(column, value)| {
            !column.notnull || column.is_ipk || !matches!(value, SqliteValue::Null)
        })
}

fn try_parse_columns_from_create_sql_ast(sql: &str) -> Option<Vec<ColumnInfo>> {
    let Statement::CreateTable(create) = parse_single_statement(sql)? else {
        return None;
    };
    columns_from_create_table_statement(&create)
}

pub(crate) fn columns_from_create_table_statement(
    create: &CreateTableStatement,
) -> Option<Vec<ColumnInfo>> {
    let CreateTableBody::Columns { columns, .. } = &create.body else {
        return None;
    };

    let mut table_pk_rowid_col_idx = None;

    if let CreateTableBody::Columns { constraints, .. } = &create.body {
        for constraint in constraints {
            match &constraint.kind {
                TableConstraintKind::PrimaryKey {
                    columns: pk_columns,
                    ..
                } if pk_columns.len() == 1 => {
                    let Some(column_name) = indexed_column_name(&pk_columns[0]) else {
                        continue;
                    };
                    let Some(index) = columns
                        .iter()
                        .position(|col| col.name.eq_ignore_ascii_case(column_name))
                    else {
                        continue;
                    };

                    let is_integer = columns[index]
                        .type_name
                        .as_ref()
                        .is_some_and(|tn| tn.name.eq_ignore_ascii_case("INTEGER"));
                    if is_integer && !create.without_rowid {
                        table_pk_rowid_col_idx = Some(index);
                    }
                }
                _ => {}
            }
        }
    }

    let rowid_col_idx = columns
        .iter()
        .enumerate()
        .find_map(|(index, col)| {
            let is_integer = col
                .type_name
                .as_ref()
                .is_some_and(|tn| tn.name.eq_ignore_ascii_case("INTEGER"));
            let pk = col.constraints.iter().find_map(|constraint| {
                if let ColumnConstraintKind::PrimaryKey { direction, .. } = &constraint.kind {
                    if *direction != Some(SortDirection::Desc) {
                        Some(())
                    } else {
                        None
                    }
                } else {
                    None
                }
            });
            if is_integer && pk.is_some() && !create.without_rowid {
                Some(index)
            } else {
                None
            }
        })
        .or(table_pk_rowid_col_idx);

    Some(
        columns
            .iter()
            .enumerate()
            .map(|(index, col)| {
                let affinity = col
                    .type_name
                    .as_ref()
                    .map_or('A', |type_name| type_to_affinity(&type_name.name));
                let type_name = col.type_name.as_ref().map(std::string::ToString::to_string);
                let is_ipk = rowid_col_idx.is_some_and(|rowid_index| rowid_index == index);
                let notnull = col.constraints.iter().any(|constraint| {
                    matches!(&constraint.kind, ColumnConstraintKind::NotNull { .. })
                });
                let has_primary_key = col.constraints.iter().any(|constraint| {
                    matches!(&constraint.kind, ColumnConstraintKind::PrimaryKey { .. })
                });
                let unique = (!is_ipk && has_primary_key)
                    || col.constraints.iter().any(|constraint| {
                        matches!(&constraint.kind, ColumnConstraintKind::Unique { .. })
                    });
                let default_value = col
                    .constraints
                    .iter()
                    .find_map(|constraint| match &constraint.kind {
                        ColumnConstraintKind::Default(default_value) => {
                            Some(format_default_value(default_value))
                        }
                        _ => None,
                    });
                let strict_type = if create.strict {
                    type_name
                        .as_deref()
                        .and_then(StrictColumnType::from_type_name)
                } else {
                    None
                };
                let (generated_expr, generated_stored) = col
                    .constraints
                    .iter()
                    .find_map(|constraint| match &constraint.kind {
                        ColumnConstraintKind::Generated { expr, storage } => {
                            let stored = storage
                                .as_ref()
                                .is_some_and(|storage| *storage == GeneratedStorage::Stored);
                            Some((Some(expr.to_string()), Some(stored)))
                        }
                        _ => None,
                    })
                    .unwrap_or((None, None));
                let collation = col.constraints.iter().find_map(|constraint| {
                    if let ColumnConstraintKind::Collate(name) = &constraint.kind {
                        Some(name.clone())
                    } else {
                        None
                    }
                });
                // Per-constraint ON CONFLICT: PRIMARY KEY clause for the rowid
                // alias, NOT NULL clause otherwise (UNIQUE conflicts live on the
                // backing index).
                let conflict_action = if is_ipk {
                    col.constraints
                        .iter()
                        .find_map(|constraint| match &constraint.kind {
                            ColumnConstraintKind::PrimaryKey { conflict, .. } => *conflict,
                            _ => None,
                        })
                } else {
                    col.constraints
                        .iter()
                        .find_map(|constraint| match &constraint.kind {
                            ColumnConstraintKind::NotNull { conflict } => *conflict,
                            _ => None,
                        })
                };

                ColumnInfo {
                    name: col.name.clone(),
                    affinity,
                    is_ipk,
                    type_name,
                    notnull,
                    unique,
                    default_value,
                    strict_type,
                    generated_expr,
                    generated_stored,
                    collation,
                    conflict_action,
                }
            })
            .collect(),
    )
}

fn parse_quoted_identifier(input: &str, quote: u8, escape: u8) -> Option<(&str, &str)> {
    let bytes = input.as_bytes();
    let mut i = 1usize;
    while i < bytes.len() {
        if bytes[i] == quote {
            if i + 1 < bytes.len() && bytes[i + 1] == escape {
                i += 2;
                continue;
            }
            return Some((&input[..=i], &input[i + 1..]));
        }
        i += 1;
    }
    None
}

fn parse_bracket_identifier(input: &str) -> Option<(&str, &str)> {
    let bytes = input.as_bytes();
    let mut i = 1usize;
    while i < bytes.len() {
        if bytes[i] == b']' {
            return Some((&input[..=i], &input[i + 1..]));
        }
        i += 1;
    }
    None
}

const COLUMN_CONSTRAINT_KEYWORDS: &[&str] = &[
    "CONSTRAINT",
    "PRIMARY",
    "NOT",
    "NULL",
    "UNIQUE",
    "CHECK",
    "DEFAULT",
    "COLLATE",
    "REFERENCES",
    "GENERATED",
    "AS",
];

/// Split a comma-separated SQL list while respecting parentheses, quotes,
/// and SQL comments.
fn split_top_level_csv_items(input: &str) -> Vec<String> {
    let mut chars = input.char_indices().peekable();
    let mut out = Vec::new();
    let mut current = String::new();
    let mut paren_depth = 0usize;
    let mut quote: Option<char> = None;
    let mut in_brackets = false;

    while let Some((_, ch)) = chars.next() {
        if let Some(q) = quote {
            current.push(ch);
            if ch == q {
                if let Some(&(_, next_ch)) = chars.peek() {
                    if next_ch == q {
                        current.push(next_ch);
                        chars.next();
                    } else {
                        quote = None;
                    }
                } else {
                    quote = None;
                }
            }
            continue;
        }

        if in_brackets {
            current.push(ch);
            if ch == ']' {
                in_brackets = false;
            }
            continue;
        }

        match ch {
            '\'' | '"' | '`' => {
                quote = Some(ch);
                current.push(ch);
            }
            '[' => {
                in_brackets = true;
                current.push(ch);
            }
            '-' if chars.peek().is_some_and(|(_, next_ch)| *next_ch == '-') => {
                chars.next();
                let ends_with_whitespace = current.chars().last().is_some_and(char::is_whitespace);
                if !current.trim_end().is_empty() && !ends_with_whitespace {
                    current.push(' ');
                }

                while let Some((_, next_ch)) = chars.next() {
                    if next_ch == '\n' {
                        break;
                    }
                    if next_ch == '\r' {
                        if chars.peek().is_some_and(|(_, next_ch)| *next_ch == '\n') {
                            chars.next();
                        }
                        break;
                    }
                }
            }
            '/' if chars.peek().is_some_and(|(_, next_ch)| *next_ch == '*') => {
                chars.next();
                let ends_with_whitespace = current.chars().last().is_some_and(char::is_whitespace);
                if !current.trim_end().is_empty() && !ends_with_whitespace {
                    current.push(' ');
                }

                let mut previous = '\0';
                for (_, next_ch) in chars.by_ref() {
                    if previous == '*' && next_ch == '/' {
                        break;
                    }
                    previous = next_ch;
                }
            }
            '(' => {
                paren_depth = paren_depth.saturating_add(1);
                current.push(ch);
            }
            ')' => {
                paren_depth = paren_depth.saturating_sub(1);
                current.push(ch);
            }
            ',' if paren_depth == 0 => {
                let part = current.trim();
                if !part.is_empty() {
                    out.push(part.to_owned());
                }
                current.clear();
            }
            _ => current.push(ch),
        }
    }

    let tail = current.trim();
    if !tail.is_empty() {
        out.push(tail.to_owned());
    }

    out
}

fn find_unquoted_name_end(input: &str) -> usize {
    let mut chars = input.char_indices().peekable();
    while let Some((idx, ch)) = chars.next() {
        if ch.is_whitespace() {
            return idx;
        }
        if ch == '-' && chars.peek().is_some_and(|(_, next_ch)| *next_ch == '-') {
            return idx;
        }
        if ch == '/' && chars.peek().is_some_and(|(_, next_ch)| *next_ch == '*') {
            return idx;
        }
    }
    input.len()
}

fn starts_with_unquoted_table_constraint(def: &str) -> bool {
    let trimmed = trim_leading_sql_space_and_comments(def);
    if trimmed.is_empty() {
        return false;
    }
    match trimmed.as_bytes()[0] {
        b'"' | b'`' | b'[' => return false,
        _ => {}
    }
    collect_unquoted_sql_keyword_tokens(trimmed)
        .first()
        .is_some_and(|(token, start)| {
            *start == 0
                && matches!(
                    token.as_str(),
                    "CONSTRAINT" | "PRIMARY" | "UNIQUE" | "CHECK" | "FOREIGN"
                )
        })
}

type SqlCharIndices<'a> = std::iter::Peekable<std::str::CharIndices<'a>>;

fn unquoted_sql_keyword_tokens(input: &str) -> Vec<String> {
    collect_unquoted_sql_keyword_tokens(input)
        .into_iter()
        .map(|(token, _)| token)
        .collect()
}

fn find_unquoted_sql_keyword(input: &str, keyword: &str) -> Option<usize> {
    let keyword = keyword.to_ascii_uppercase();
    collect_unquoted_sql_keyword_tokens(input)
        .into_iter()
        .find_map(|(token, start)| (token == keyword).then_some(start))
}

fn find_top_level_unquoted_sql_keyword(input: &str, keyword: &str) -> Option<usize> {
    let mut chars = input.char_indices().peekable();
    let mut paren_depth = 0_usize;
    let mut token_start = None;

    while let Some((idx, ch)) = chars.next() {
        let is_token_char = ch.is_ascii_alphanumeric() || ch == '_';
        if paren_depth == 0 && is_token_char {
            token_start.get_or_insert(idx);
            continue;
        }
        if let Some(start) = token_start.take()
            && input[start..idx].eq_ignore_ascii_case(keyword)
        {
            return Some(start);
        }

        match ch {
            '\'' | '"' | '`' => skip_quoted_sql(&mut chars, ch),
            '[' => skip_bracket_identifier(&mut chars),
            '-' if chars.peek().is_some_and(|(_, next_ch)| *next_ch == '-') => {
                let _ = chars.next();
                skip_line_comment(&mut chars);
            }
            '/' if chars.peek().is_some_and(|(_, next_ch)| *next_ch == '*') => {
                let _ = chars.next();
                skip_block_comment(&mut chars);
            }
            '(' => paren_depth += 1,
            ')' => paren_depth = paren_depth.saturating_sub(1),
            _ => {}
        }
    }

    token_start.filter(|start| input[*start..].eq_ignore_ascii_case(keyword))
}

fn find_unquoted_sql_char(input: &str, target: char) -> Option<usize> {
    let mut chars = input.char_indices().peekable();
    while let Some((idx, ch)) = chars.next() {
        match ch {
            '\'' | '"' | '`' => skip_quoted_sql(&mut chars, ch),
            '[' => skip_bracket_identifier(&mut chars),
            '-' if chars.peek().is_some_and(|(_, next_ch)| *next_ch == '-') => {
                let _ = chars.next();
                skip_line_comment(&mut chars);
            }
            '/' if chars.peek().is_some_and(|(_, next_ch)| *next_ch == '*') => {
                let _ = chars.next();
                skip_block_comment(&mut chars);
            }
            _ if ch == target => return Some(idx),
            _ => {}
        }
    }
    None
}

fn find_matching_sql_paren(input: &str, open_idx: usize) -> Option<usize> {
    if input.as_bytes().get(open_idx).copied() != Some(b'(') {
        return None;
    }

    let mut depth = 0_usize;
    let mut chars = input[open_idx..].char_indices().peekable();
    while let Some((rel_idx, ch)) = chars.next() {
        let idx = open_idx + rel_idx;
        match ch {
            '\'' | '"' | '`' => skip_quoted_sql(&mut chars, ch),
            '[' => skip_bracket_identifier(&mut chars),
            '-' if chars.peek().is_some_and(|(_, next_ch)| *next_ch == '-') => {
                let _ = chars.next();
                skip_line_comment(&mut chars);
            }
            '/' if chars.peek().is_some_and(|(_, next_ch)| *next_ch == '*') => {
                let _ = chars.next();
                skip_block_comment(&mut chars);
            }
            '(' => depth += 1,
            ')' => {
                depth = depth.checked_sub(1)?;
                if depth == 0 {
                    return Some(idx);
                }
            }
            _ => {}
        }
    }
    None
}

fn trim_leading_sql_space_and_comments(mut input: &str) -> &str {
    loop {
        let trimmed = input.trim_start();
        if let Some(rest) = trimmed.strip_prefix("--") {
            let end = rest.find(['\n', '\r']).map_or(rest.len(), |idx| idx + 1);
            input = &rest[end..];
            continue;
        }
        if let Some(rest) = trimmed.strip_prefix("/*") {
            let Some(end) = rest.find("*/") else {
                return "";
            };
            input = &rest[end + 2..];
            continue;
        }
        return trimmed;
    }
}

fn collect_unquoted_sql_keyword_tokens(input: &str) -> Vec<(String, usize)> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut current_start = 0_usize;
    let mut chars = input.char_indices().peekable();

    while let Some((idx, ch)) = chars.next() {
        match ch {
            '\'' | '"' | '`' => {
                push_keyword_token(&mut tokens, &mut current, current_start);
                skip_quoted_sql(&mut chars, ch);
            }
            '[' => {
                push_keyword_token(&mut tokens, &mut current, current_start);
                skip_bracket_identifier(&mut chars);
            }
            '-' if chars.peek().is_some_and(|(_, next_ch)| *next_ch == '-') => {
                let _ = chars.next();
                push_keyword_token(&mut tokens, &mut current, current_start);
                skip_line_comment(&mut chars);
            }
            '/' if chars.peek().is_some_and(|(_, next_ch)| *next_ch == '*') => {
                let _ = chars.next();
                push_keyword_token(&mut tokens, &mut current, current_start);
                skip_block_comment(&mut chars);
            }
            _ if ch.is_ascii_alphanumeric() || matches!(ch, '_') => {
                if current.is_empty() {
                    current_start = idx;
                }
                current.push(ch.to_ascii_uppercase());
            }
            _ => push_keyword_token(&mut tokens, &mut current, current_start),
        }
    }

    push_keyword_token(&mut tokens, &mut current, current_start);
    tokens
}

fn push_keyword_token(
    tokens: &mut Vec<(String, usize)>,
    current: &mut String,
    current_start: usize,
) {
    if !current.is_empty() {
        tokens.push((std::mem::take(current), current_start));
    }
}

fn skip_quoted_sql(chars: &mut SqlCharIndices<'_>, quote: char) {
    while let Some((_, ch)) = chars.next() {
        if ch != quote {
            continue;
        }
        if chars.peek().is_some_and(|(_, next_ch)| *next_ch == quote) {
            let _ = chars.next();
        } else {
            break;
        }
    }
}

fn skip_bracket_identifier(chars: &mut SqlCharIndices<'_>) {
    for (_, ch) in chars.by_ref() {
        if ch == ']' {
            break;
        }
    }
}

fn skip_line_comment(chars: &mut SqlCharIndices<'_>) {
    for (_, ch) in chars.by_ref() {
        if ch == '\n' || ch == '\r' {
            break;
        }
    }
}

fn skip_block_comment(chars: &mut SqlCharIndices<'_>) {
    let mut previous = '\0';
    for (_, ch) in chars.by_ref() {
        if previous == '*' && ch == '/' {
            break;
        }
        previous = ch;
    }
}

fn unquoted_tokens_contain_phrase(tokens: &[String], phrase: &[&str]) -> bool {
    !phrase.is_empty()
        && tokens.len() >= phrase.len()
        && tokens.windows(phrase.len()).any(|window| {
            window
                .iter()
                .zip(phrase)
                .all(|(token, expected)| token.as_str() == *expected)
        })
}

fn extract_collation_name(remainder: &str) -> Option<String> {
    let raw_name = remainder.get(find_collation_name_range(remainder)?)?;
    let name = strip_sql_name_quotes(raw_name);
    (!name.is_empty()).then(|| name.to_ascii_uppercase())
}

fn find_collation_name_range(remainder: &str) -> Option<std::ops::Range<usize>> {
    let pos = find_unquoted_sql_keyword(remainder, "COLLATE")?;
    let after = trim_leading_sql_space_and_comments(&remainder[pos + 7..]);
    let start = remainder.len().checked_sub(after.len())?;
    let bytes = after.as_bytes();
    if bytes.is_empty() {
        return None;
    }

    let raw_len = match bytes[0] {
        b'\'' => parse_quoted_identifier(after, b'\'', b'\'')?.0,
        b'"' => parse_quoted_identifier(after, b'"', b'"')?.0,
        b'`' => parse_quoted_identifier(after, b'`', b'`')?.0,
        b'[' => parse_bracket_identifier(after)?.0,
        _ => {
            let end = after
                .find(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '_'))
                .unwrap_or(after.len());
            &after[..end]
        }
    }
    .len();
    (raw_len > 0).then_some(start..start + raw_len)
}

fn strip_sql_name_quotes(token: &str) -> String {
    let trimmed = token.trim();
    if trimmed.len() >= 2 {
        if trimmed.starts_with('\'') && trimmed.ends_with('\'') {
            return trimmed[1..trimmed.len() - 1].replace("''", "'");
        }
        return strip_identifier_quotes(trimmed);
    }
    trimmed.to_owned()
}

fn strip_identifier_quotes(token: &str) -> String {
    let trimmed = token.trim();
    if trimmed.len() >= 2 {
        if trimmed.starts_with('"') && trimmed.ends_with('"') {
            return trimmed[1..trimmed.len() - 1].replace("\"\"", "\"");
        }
        if trimmed.starts_with('`') && trimmed.ends_with('`') {
            return trimmed[1..trimmed.len() - 1].replace("``", "`");
        }
        if trimmed.starts_with('[') && trimmed.ends_with(']') {
            return trimmed[1..trimmed.len() - 1].to_owned();
        }
    }
    trimmed.to_owned()
}

fn extract_type_declaration(tokens: &[&str]) -> String {
    let mut parts = Vec::new();
    let mut paren_depth = 0isize;
    for token in tokens {
        let token_upper = token
            .trim_matches(|c: char| c == ',' || c == ';')
            .to_ascii_uppercase();
        if paren_depth == 0 && COLUMN_CONSTRAINT_KEYWORDS.contains(&token_upper.as_str()) {
            break;
        }
        parts.push(*token);
        for ch in token.chars() {
            if ch == '(' {
                paren_depth += 1;
            } else if ch == ')' && paren_depth > 0 {
                paren_depth -= 1;
            }
        }
    }
    parts.join(" ")
}

fn extract_generated_column_clause(remainder: &str) -> (Option<String>, Option<bool>) {
    let Some(as_pos) = find_top_level_unquoted_sql_keyword(remainder, "AS") else {
        return (None, None);
    };
    let after_keyword = &remainder[as_pos + "AS".len()..];
    let after_space_and_comments = trim_leading_sql_space_and_comments(after_keyword);
    if !after_space_and_comments.starts_with('(') {
        return (None, None);
    }
    let skipped = after_keyword.len() - after_space_and_comments.len();
    let open_paren = as_pos + "AS".len() + skipped;
    let Some(close_paren) = find_matching_sql_paren(remainder, open_paren) else {
        return (None, None);
    };

    let tail = trim_leading_sql_space_and_comments(&remainder[close_paren + 1..]);
    let is_stored = collect_unquoted_sql_keyword_tokens(tail)
        .first()
        .is_some_and(|(token, start)| *start == 0 && token == "STORED");

    (
        Some(remainder[open_paren + 1..close_paren].trim().to_owned()),
        Some(is_stored),
    )
}

/// Extract a DEFAULT value from a column definition remainder (the part after
/// the column name).  Handles `DEFAULT literal`, `DEFAULT -number`,
/// `DEFAULT 'string'`, `DEFAULT "string"`, and `DEFAULT (expr)`.
fn extract_default_value(remainder: &str) -> Option<String> {
    let pos = find_unquoted_sql_keyword(remainder, "DEFAULT")?;
    let after = trim_leading_sql_space_and_comments(&remainder[pos + 7..]);
    if after.is_empty() {
        return None;
    }
    // Parenthesized expression: DEFAULT (...)
    if after.starts_with('(') {
        let mut depth = 0i32;
        let bytes = after.as_bytes();
        let mut idx = 0_usize;
        while idx < bytes.len() {
            match bytes[idx] {
                quote @ (b'\'' | b'"') => {
                    idx += 1;
                    while idx < bytes.len() {
                        if bytes[idx] == quote {
                            if idx + 1 < bytes.len() && bytes[idx + 1] == quote {
                                idx += 2;
                            } else {
                                idx += 1;
                                break;
                            }
                        } else {
                            idx += 1;
                        }
                    }
                    continue;
                }
                b'(' => depth += 1,
                b')' => {
                    depth -= 1;
                    if depth == 0 {
                        return Some(after[..=idx].to_owned());
                    }
                    if depth < 0 {
                        return None;
                    }
                }
                _ => {}
            }
            idx += 1;
        }
        return None;
    }
    // Quoted string: DEFAULT '...' or DEFAULT "..."
    if let Some(quote) = after
        .as_bytes()
        .first()
        .copied()
        .filter(|quote| matches!(*quote, b'\'' | b'"'))
    {
        let rest = &after[1..];
        let mut i = 0;
        let bytes = rest.as_bytes();
        while i < bytes.len() {
            if bytes[i] == quote {
                if i + 1 < bytes.len() && bytes[i + 1] == quote {
                    i += 2;
                    continue;
                }
                return Some(after[..i + 2].to_owned());
            }
            i += 1;
        }
        return None;
    }
    // Unquoted token: DEFAULT NULL, DEFAULT 0, DEFAULT -1, DEFAULT CURRENT_TIMESTAMP
    let end = after
        .find(|c: char| c.is_ascii_whitespace() || c == ',')
        .unwrap_or(after.len());
    let token = &after[..end];
    if token.is_empty() {
        None
    } else {
        Some(token.to_owned())
    }
}

/// Map a SQL type keyword to an affinity character.
fn type_to_affinity(type_str: &str) -> char {
    // SQLite affinity rules (section 3.1 of datatype3.html):
    // Priority: INT > TEXT/CHAR/CLOB > BLOB/empty > REAL/FLOA/DOUB > NUMERIC
    let upper = type_str.to_uppercase();
    if upper.contains("INT") {
        'D' // INTEGER affinity
    } else if upper.contains("TEXT") || upper.contains("CHAR") || upper.contains("CLOB") {
        'B' // TEXT affinity
    } else if upper.contains("BLOB") || upper.is_empty() {
        'A' // BLOB (none) affinity
    } else if upper.contains("REAL") || upper.contains("FLOA") || upper.contains("DOUB") {
        'E' // REAL affinity
    } else {
        'C' // NUMERIC affinity
    }
}

// ── Tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::process::{Command, Stdio};

    async fn persist_test_db(
        path: &Path,
        schema: &[TableSchema],
        db: &MemDatabase,
        schema_cookie: u32,
        change_counter: u32,
    ) -> Result<()> {
        let cx = Cx::new();
        persist_to_sqlite(&cx, path, schema, db, schema_cookie, change_counter).await
    }

    async fn load_test_db(path: &Path) -> Result<LoadedState> {
        let cx = Cx::new();
        load_from_sqlite(&cx, path).await
    }

    fn assert_schema_root_owner_collision(error: FrankenError, owner_names: &[&str]) {
        let FrankenError::DatabaseCorrupt { detail } = error else {
            panic!("unexpected schema-root-alias error: {error:?}");
        };
        assert!(
            detail.contains("B-tree root page") && detail.contains("multiple owners"),
            "schema-root-alias error must identify the ownership collision: {detail}"
        );
        for owner_name in owner_names {
            assert!(
                detail.contains(owner_name),
                "schema-root-alias error must identify owner `{owner_name}`: {detail}"
            );
        }
    }

    async fn assert_compat_rejects_catalog_corruption(path: &Path, expected_details: &[&str]) {
        let error = load_test_db(path)
            .await
            .expect_err("malformed sqlite_master ownership must fail the compat loader");
        let FrankenError::DatabaseCorrupt { detail } = error else {
            panic!("unexpected malformed-catalog error: {error:?}");
        };
        for expected_detail in expected_details {
            assert!(
                detail.contains(expected_detail),
                "malformed-catalog error must contain `{expected_detail}`: {detail}"
            );
        }
    }

    #[test]
    fn test_compat_catalog_gate_rejects_truncated_record_shapes() {
        let complete = vec![
            SqliteValue::Text("table".into()),
            SqliteValue::Text("docs".into()),
            SqliteValue::Text("docs".into()),
            SqliteValue::Integer(2),
            SqliteValue::Text("CREATE TABLE docs(id INTEGER PRIMARY KEY)".into()),
        ];
        for truncated_len in [3, 4] {
            let error = validate_sqlite_schema_catalog_from_master_entries(&[complete
                [..truncated_len]
                .to_vec()])
            .expect_err("the compat catalog gate must reject truncated records");
            let FrankenError::DatabaseCorrupt { detail } = error else {
                panic!("unexpected truncated-catalog error: {error:?}");
            };
            assert!(
                detail.contains(&format!("{truncated_len} columns"))
                    && detail.contains("exactly 5"),
                "truncated-catalog error must identify the record shape: {detail}"
            );
        }
    }

    #[test]
    fn test_load_from_sqlite_accepts_check_conflict_and_nullable_no_storage_roots() {
        asupersync::test_utils::run_test(|| async {
            let dir = tempfile::tempdir().unwrap();
            let db_path = dir.path().join("compat_valid_check_conflict.db");
            {
                let sqlite = rusqlite::Connection::open(&db_path).unwrap();
                sqlite
                    .execute_batch(
                        "CREATE TABLE parent(id INTEGER PRIMARY KEY);
                         CREATE TABLE docs(
                             id INTEGER PRIMARY KEY,
                             parent_id INTEGER REFERENCES parent(id),
                             value INTEGER UNIQUE,
                             CHECK(value > 0) ON CONFLICT FAIL
                         );
                         CREATE VIEW docs_view AS SELECT value FROM docs;
                         CREATE TRIGGER docs_trigger
                         AFTER INSERT ON docs BEGIN SELECT 1; END;
                         CREATE VIRTUAL TABLE docs_search USING fts5(content);
                         INSERT INTO parent VALUES (1);
                         INSERT INTO docs VALUES (1, 1, 7);
                         PRAGMA writable_schema = ON;
                         UPDATE sqlite_schema
                         SET rootpage = NULL
                         WHERE type IN ('view', 'trigger')
                            OR (type = 'table' AND name = 'docs_search');
                         PRAGMA writable_schema = OFF;",
                    )
                    .unwrap();
            }

            let loaded = load_test_db(&db_path)
                .await
                .expect("compat loading must accept valid stored SQLite DDL");
            let docs = loaded
                .schema
                .iter()
                .find(|table| table.name.eq_ignore_ascii_case("docs"))
                .expect("compat loading must publish the ordinary table");
            let rows = loaded
                .db
                .get_table(docs.root_page)
                .expect("compat loading must attach the ordinary table root")
                .iter_rows()
                .collect::<Vec<_>>();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].1[2], SqliteValue::Integer(7));
        });
    }

    #[test]
    fn test_load_from_sqlite_rejects_catalog_sql_identity_and_duplicate_corruption() {
        asupersync::test_utils::run_test(|| async {
            let cases: &[(&str, &str, &str, &[&str])] = &[
                (
                    "table_sql_null",
                    "CREATE TABLE docs(id INTEGER PRIMARY KEY);",
                    "UPDATE sqlite_schema SET sql = NULL
                     WHERE type = 'table' AND name = 'docs';",
                    &["table", "docs", "NULL SQL"],
                ),
                (
                    "table_sql_blob",
                    "CREATE TABLE docs(id INTEGER PRIMARY KEY);",
                    "UPDATE sqlite_schema SET sql = X'00'
                     WHERE type = 'table' AND name = 'docs';",
                    &["table", "docs", "unsupported SQL metadata"],
                ),
                (
                    "malformed_table_sql",
                    "CREATE TABLE docs(id INTEGER PRIMARY KEY);",
                    "UPDATE sqlite_schema SET sql = 'CREATE TABLE docs ('
                     WHERE type = 'table' AND name = 'docs';",
                    &["table", "docs", "could not be parsed"],
                ),
                (
                    "malformed_virtual_table_sql",
                    "CREATE VIRTUAL TABLE docs USING fts5(content);",
                    "UPDATE sqlite_schema SET sql = 'CREATE VIRTUAL TABLE docs USING'
                     WHERE type = 'table' AND name = 'docs';",
                    &["table", "docs", "could not be parsed"],
                ),
                (
                    "orphan_implicit_index",
                    "CREATE TABLE wr(
                         id TEXT PRIMARY KEY,
                         payload TEXT UNIQUE
                     ) WITHOUT ROWID;",
                    "UPDATE sqlite_schema SET tbl_name = 'missing_wr'
                     WHERE type = 'index' AND name = 'sqlite_autoindex_wr_2';",
                    &["sqlite_autoindex_wr_2", "missing table", "missing_wr"],
                ),
                (
                    "table_catalog_owner_mismatch",
                    "CREATE TABLE docs(id INTEGER PRIMARY KEY);",
                    "UPDATE sqlite_schema SET tbl_name = 'other'
                     WHERE type = 'table' AND name = 'docs';",
                    &["table", "docs", "other"],
                ),
                (
                    "table_declared_name_mismatch",
                    "CREATE TABLE docs(id INTEGER PRIMARY KEY);",
                    "UPDATE sqlite_schema SET sql = 'CREATE TABLE other(id INTEGER PRIMARY KEY)'
                     WHERE type = 'table' AND name = 'docs';",
                    &["table", "docs", "different object name", "other"],
                ),
                (
                    "index_declared_name_mismatch",
                    "CREATE TABLE a(x INTEGER);
                     CREATE INDEX idx_catalog ON a(x);",
                    "UPDATE sqlite_schema SET sql = 'CREATE INDEX idx_sql ON a(x)'
                     WHERE type = 'index' AND name = 'idx_catalog';",
                    &["index", "idx_catalog", "different object name", "idx_sql"],
                ),
                (
                    "index_declared_table_mismatch",
                    "CREATE TABLE a(x INTEGER);
                     CREATE TABLE b(x INTEGER);
                     CREATE INDEX idx_catalog ON a(x);",
                    "UPDATE sqlite_schema SET sql = 'CREATE INDEX idx_catalog ON b(x)'
                     WHERE type = 'index' AND name = 'idx_catalog';",
                    &["idx_catalog", "names table", "a", "declares table", "b"],
                ),
                (
                    "index_fallback_text",
                    "CREATE TABLE a(x INTEGER);
                     CREATE INDEX idx_catalog ON a(x);",
                    "UPDATE sqlite_schema SET sql = 'nonsense ON a(x)'
                     WHERE type = 'index' AND name = 'idx_catalog';",
                    &["index", "idx_catalog", "could not be parsed"],
                ),
                (
                    "qualified_table_sql",
                    "CREATE TABLE docs(id INTEGER PRIMARY KEY);",
                    "UPDATE sqlite_schema
                     SET sql = 'CREATE TABLE temp.docs(id INTEGER PRIMARY KEY)'
                     WHERE type = 'table' AND name = 'docs';",
                    &["table", "docs", "schema-qualified object", "temp"],
                ),
                (
                    "duplicate_table_column",
                    "CREATE TABLE docs(id INTEGER, value TEXT);",
                    "UPDATE sqlite_schema
                     SET sql = 'CREATE TABLE docs(id INTEGER, ID TEXT)'
                     WHERE type = 'table' AND name = 'docs';",
                    &["docs", "semantic validation", "duplicate column"],
                ),
                (
                    "index_missing_column",
                    "CREATE TABLE docs(value INTEGER);
                     CREATE INDEX docs_idx ON docs(value);",
                    "UPDATE sqlite_schema
                     SET sql = 'CREATE INDEX docs_idx ON docs(missing_column)'
                     WHERE type = 'index' AND name = 'docs_idx';",
                    &["docs_idx", "semantic validation", "no such column"],
                ),
                (
                    "index_on_virtual_table",
                    "CREATE TABLE base(value INTEGER);
                     CREATE INDEX base_idx ON base(value);
                     CREATE VIRTUAL TABLE docs_search USING fts5(content);",
                    "UPDATE sqlite_schema
                     SET tbl_name = 'docs_search',
                         sql = 'CREATE INDEX base_idx ON docs_search(content)'
                     WHERE type = 'index' AND name = 'base_idx';",
                    &["base_idx", "virtual table", "docs_search"],
                ),
                (
                    "trigger_on_virtual_table",
                    "CREATE TABLE base(value INTEGER);
                     CREATE TRIGGER base_trigger
                     AFTER INSERT ON base BEGIN SELECT 1; END;
                     CREATE VIRTUAL TABLE docs_search USING fts5(content);",
                    "UPDATE sqlite_schema
                     SET tbl_name = 'docs_search',
                         sql = 'CREATE TRIGGER base_trigger AFTER INSERT ON docs_search BEGIN SELECT 1; END;'
                     WHERE type = 'trigger' AND name = 'base_trigger';",
                    &["base_trigger", "virtual table", "docs_search"],
                ),
                (
                    "duplicate_index_identity",
                    "CREATE TABLE a(x INTEGER, y INTEGER);
                     CREATE INDEX idx_a ON a(x);
                     CREATE INDEX idx_b ON a(y);",
                    "UPDATE sqlite_schema SET name = 'IDX_A'
                     WHERE type = 'index' AND name = 'idx_b';",
                    &["duplicate relation/index identity", "idx_a"],
                ),
            ];

            for (case_name, setup_sql, mutation_sql, expected_details) in cases {
                let dir = tempfile::tempdir().unwrap();
                let db_path = dir.path().join(format!("compat_{case_name}.db"));
                {
                    let sqlite = rusqlite::Connection::open(&db_path).unwrap();
                    sqlite.execute_batch(setup_sql).unwrap();
                    sqlite
                        .execute_batch("PRAGMA writable_schema = ON;")
                        .unwrap();
                    sqlite.execute_batch(mutation_sql).unwrap();
                    sqlite
                        .execute_batch("PRAGMA writable_schema = OFF;")
                        .unwrap();
                }

                let error = load_test_db(&db_path)
                    .await
                    .expect_err("compat load must reject malformed catalog metadata");
                let FrankenError::DatabaseCorrupt { detail } = error else {
                    panic!("{case_name}: unexpected malformed-catalog error: {error:?}");
                };
                for expected_detail in *expected_details {
                    assert!(
                        detail
                            .to_ascii_lowercase()
                            .contains(&expected_detail.to_ascii_lowercase()),
                        "{case_name}: catalog error must contain `{expected_detail}`: {detail}"
                    );
                }
            }
        });
    }

    #[test]
    fn test_parse_loaded_default_text_requires_complete_quoted_literal() {
        assert_eq!(
            parse_loaded_column_default_value("'can''t'"),
            SqliteValue::Text("can't".into()),
        );
        assert_eq!(
            parse_loaded_column_default_value(r#""a""b""#),
            SqliteValue::Text("a\"b".into()),
        );
        assert_eq!(
            parse_loaded_column_default_value("'x' || 'y'"),
            SqliteValue::Text("'x' || 'y'".into()),
        );
        assert_eq!(
            parse_loaded_column_default_value("('a)b')"),
            SqliteValue::Text("a)b".into()),
        );
        assert_eq!(
            parse_loaded_column_default_value(r#"("a)b")"#),
            SqliteValue::Text("a)b".into()),
        );
        assert_eq!(
            extract_default_value("TEXT DEFAULT ('a)b')").as_deref(),
            Some("('a)b')")
        );
        assert_eq!(
            extract_default_value(r#"TEXT DEFAULT ("a)b")"#).as_deref(),
            Some(r#"("a)b")"#)
        );
        assert_eq!(
            extract_default_value("TEXT CHECK (note <> 'DEFAULT bad') DEFAULT 'ok'").as_deref(),
            Some("'ok'")
        );
        assert_eq!(
            extract_default_value("TEXT CHECK (note <> 'DEFAULT bad')").as_deref(),
            None
        );
        assert_eq!(
            extract_default_value("TEXT /* DEFAULT 'bad' */ DEFAULT 'ok'").as_deref(),
            Some("'ok'")
        );
        assert_eq!(
            extract_default_value("TEXT DEFAULT /* comment */ 'ok'").as_deref(),
            Some("'ok'")
        );
        assert_eq!(
            extract_default_value("TEXT DEFAULT -- comment\n 'ok'").as_deref(),
            Some("'ok'")
        );
    }

    fn make_test_schema_and_db() -> (Vec<TableSchema>, MemDatabase) {
        let mut db = MemDatabase::new();
        let root = db.create_table(2);
        let table = db.tables.get_mut(&root).unwrap();
        table.insert_row(
            1,
            vec![SqliteValue::Integer(42), SqliteValue::Text("hello".into())],
        );
        table.insert_row(
            2,
            vec![SqliteValue::Integer(99), SqliteValue::Text("world".into())],
        );

        let schema = vec![TableSchema {
            name: "test_table".to_owned(),
            root_page: root,
            columns: vec![
                ColumnInfo {
                    name: "id".to_owned(),
                    affinity: 'd',
                    is_ipk: false,
                    type_name: None,
                    notnull: false,
                    unique: false,
                    default_value: None,
                    strict_type: None,
                    generated_expr: None,
                    generated_stored: None,
                    collation: None,
                    conflict_action: None,
                },
                ColumnInfo {
                    name: "name".to_owned(),
                    affinity: 'C',
                    is_ipk: false,
                    type_name: None,
                    notnull: false,
                    unique: false,
                    default_value: None,
                    strict_type: None,
                    generated_expr: None,
                    generated_stored: None,
                    collation: None,
                    conflict_action: None,
                },
            ],
            indexes: Vec::new(),
            strict: false,
            without_rowid: false,
            primary_key_constraints: Vec::new(),
            foreign_keys: Vec::new(),
            check_constraints: Vec::new(),
        }];

        (schema, db)
    }

    #[test]
    fn test_roundtrip_persist_and_load() {
        asupersync::test_utils::run_test(|| async {
            let dir = tempfile::tempdir().unwrap();
            let db_path = dir.path().join("test.db");

            let (schema, db) = make_test_schema_and_db();
            persist_test_db(&db_path, &schema, &db, 0, 0).await.unwrap();

            assert!(db_path.exists(), "db file should exist");
            assert!(is_sqlite_format(&db_path), "should have SQLite magic");

            let loaded = load_test_db(&db_path).await.unwrap();
            assert_eq!(loaded.schema.len(), 1);
            assert_eq!(loaded.schema[0].name, "test_table");
            assert_eq!(loaded.schema[0].columns.len(), 2);

            let table = loaded.db.get_table(loaded.schema[0].root_page).unwrap();
            let rows: Vec<_> = table.iter_rows().collect();
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].0, 1); // rowid
            assert_eq!(rows[0].1[0], SqliteValue::Integer(42));
            assert_eq!(rows[0].1[1], SqliteValue::Text("hello".into()));
            assert_eq!(rows[1].0, 2);
            assert_eq!(rows[1].1[0], SqliteValue::Integer(99));
            assert_eq!(rows[1].1[1], SqliteValue::Text("world".into()));
        });
    }

    #[test]
    fn test_empty_database_roundtrip() {
        asupersync::test_utils::run_test(|| async {
            let dir = tempfile::tempdir().unwrap();
            let db_path = dir.path().join("empty.db");

            let schema: Vec<TableSchema> = Vec::new();
            let db = MemDatabase::new();
            persist_test_db(&db_path, &schema, &db, 0, 0).await.unwrap();

            assert!(is_sqlite_format(&db_path));

            let loaded = load_test_db(&db_path).await.unwrap();
            assert!(loaded.schema.is_empty());
        });
    }

    #[test]
    fn test_persist_creates_sqlite3_readable_file() {
        asupersync::test_utils::run_test(|| async {
            let dir = tempfile::tempdir().unwrap();
            let db_path = dir.path().join("readable.db");

            let (schema, db) = make_test_schema_and_db();
            persist_test_db(&db_path, &schema, &db, 0, 0).await.unwrap();

            // Verify with rusqlite (C SQLite) that the file is valid.
            let conn = rusqlite::Connection::open(&db_path).unwrap();
            let mut stmt = conn
                .prepare("SELECT id, name FROM test_table ORDER BY id")
                .unwrap();
            let rows: Vec<(i64, String)> = stmt
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
                .unwrap()
                .collect::<std::result::Result<Vec<_>, _>>()
                .unwrap();

            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0], (42, "hello".to_owned()));
            assert_eq!(rows[1], (99, "world".to_owned()));
        });
    }

    #[test]
    fn test_without_rowid_storage_layout_reorders_rows_and_deduplicates_pk_locators() {
        let sql = "CREATE TABLE wr(
            payload TEXT,
            tenant TEXT COLLATE NOCASE,
            seq INTEGER,
            shard TEXT COLLATE RTRIM,
            note TEXT,
            PRIMARY KEY(shard DESC, tenant ASC)
        ) WITHOUT ROWID";
        let Some(Statement::CreateTable(create)) = parse_single_statement(sql) else {
            panic!("test CREATE TABLE must parse");
        };
        let columns = columns_from_create_table_statement(&create).unwrap();
        let specs = plan_implicit_constraint_indexes(&create, "wr").unwrap();
        let layout = without_rowid_storage_layout("wr", &columns, &specs).unwrap();

        assert_eq!(layout.storage_to_declared, vec![3, 1, 0, 2, 4]);
        assert_eq!(layout.primary_key_declared, vec![3, 1]);
        assert_eq!(
            layout.primary_key_sort_directions,
            vec![SortDirection::Desc, SortDirection::Asc]
        );
        assert_eq!(
            layout.primary_key_collations,
            vec![Some("RTRIM".to_owned()), Some("NOCASE".to_owned())]
        );

        let declared = vec![
            SqliteValue::Text("payload".into()),
            SqliteValue::Text("Tenant".into()),
            SqliteValue::Integer(7),
            SqliteValue::Text("shard ".into()),
            SqliteValue::Text("note".into()),
        ];
        let storage = layout
            .storage_values_from_declared("wr", &declared)
            .unwrap();
        assert_eq!(
            storage,
            vec![
                SqliteValue::Text("shard ".into()),
                SqliteValue::Text("Tenant".into()),
                SqliteValue::Text("payload".into()),
                SqliteValue::Integer(7),
                SqliteValue::Text("note".into()),
            ]
        );
        assert_eq!(
            layout
                .declared_values_from_storage("wr", &storage, &columns)
                .unwrap(),
            declared
        );

        let tenant_binary = IndexSchema {
            name: "idx_tenant_binary".to_owned(),
            root_page: 0,
            columns: vec!["tenant".to_owned()],
            key_expressions: Vec::new(),
            key_sort_directions: vec![SortDirection::Asc],
            where_clause: None,
            is_unique: false,
            key_collations: vec![Some("BINARY".to_owned())],
            conflict_action: None,
        };
        let tenant_binary_layout = without_rowid_secondary_index_layout(
            &TableSchema {
                name: "wr".to_owned(),
                root_page: 0,
                columns: columns.clone(),
                indexes: Vec::new(),
                strict: false,
                without_rowid: true,
                primary_key_constraints: vec![PrimaryKeyConstraint::new(
                    vec!["shard".to_owned(), "tenant".to_owned()],
                    vec![SortDirection::Desc, SortDirection::Asc],
                    vec![Some("RTRIM".to_owned()), Some("NOCASE".to_owned())],
                )],
                foreign_keys: Vec::new(),
                check_constraints: Vec::new(),
            },
            &tenant_binary,
            &layout,
            false,
        )
        .unwrap();
        assert_eq!(tenant_binary_layout.logical_declared, vec![1]);
        assert_eq!(tenant_binary_layout.locator_declared, vec![3, 1]);
        assert_eq!(
            tenant_binary_layout.cursor_desc_flags,
            vec![false, true, false]
        );
        assert_eq!(
            tenant_binary_layout.cursor_collations,
            vec![
                Some("BINARY".to_owned()),
                Some("RTRIM".to_owned()),
                Some("NOCASE".to_owned()),
            ]
        );
        let mut implicit_tenant_binary = tenant_binary.clone();
        implicit_tenant_binary.name = "sqlite_autoindex_wr_2".to_owned();
        implicit_tenant_binary.is_unique = true;
        let implicit_tenant_binary_layout = without_rowid_secondary_index_layout(
            &TableSchema {
                name: "wr".to_owned(),
                root_page: 0,
                columns: columns.clone(),
                indexes: Vec::new(),
                strict: false,
                without_rowid: true,
                primary_key_constraints: vec![PrimaryKeyConstraint::new(
                    vec!["shard".to_owned(), "tenant".to_owned()],
                    vec![SortDirection::Desc, SortDirection::Asc],
                    vec![Some("RTRIM".to_owned()), Some("NOCASE".to_owned())],
                )],
                foreign_keys: Vec::new(),
                check_constraints: Vec::new(),
            },
            &implicit_tenant_binary,
            &layout,
            true,
        )
        .unwrap();
        assert_eq!(
            implicit_tenant_binary_layout.cursor_desc_flags,
            vec![false, false, false]
        );
        assert_eq!(
            implicit_tenant_binary_layout.cursor_collations,
            tenant_binary_layout.cursor_collations
        );

        let shard_asc = IndexSchema {
            name: "idx_shard_asc".to_owned(),
            root_page: 0,
            columns: vec!["shard".to_owned()],
            key_expressions: Vec::new(),
            key_sort_directions: vec![SortDirection::Asc],
            where_clause: None,
            is_unique: false,
            key_collations: vec![Some("RTRIM".to_owned())],
            conflict_action: None,
        };
        let table = TableSchema {
            name: "wr".to_owned(),
            root_page: 0,
            columns,
            indexes: Vec::new(),
            strict: false,
            without_rowid: true,
            primary_key_constraints: vec![PrimaryKeyConstraint::new(
                vec!["shard".to_owned(), "tenant".to_owned()],
                vec![SortDirection::Desc, SortDirection::Asc],
                vec![Some("RTRIM".to_owned()), Some("NOCASE".to_owned())],
            )],
            foreign_keys: Vec::new(),
            check_constraints: Vec::new(),
        };
        let shard_asc_layout =
            without_rowid_secondary_index_layout(&table, &shard_asc, &layout, false).unwrap();
        assert_eq!(shard_asc_layout.logical_declared, vec![3]);
        assert_eq!(shard_asc_layout.locator_declared, vec![1]);
        assert_eq!(shard_asc_layout.cursor_desc_flags, vec![false, false]);
        assert_eq!(
            shard_asc_layout.cursor_collations,
            vec![Some("RTRIM".to_owned()), Some("NOCASE".to_owned())]
        );
    }

    #[test]
    fn test_without_rowid_storage_layout_collapses_only_equivalent_repeated_pk_terms() {
        let sql = "CREATE TABLE wr(
            key TEXT,
            seq INTEGER,
            payload TEXT,
            PRIMARY KEY(
                key COLLATE NOCASE ASC,
                key COLLATE nocase DESC,
                key COLLATE BINARY DESC,
                seq ASC
            )
        ) WITHOUT ROWID";
        let Some(Statement::CreateTable(create)) = parse_single_statement(sql) else {
            panic!("test CREATE TABLE must parse");
        };
        let columns = columns_from_create_table_statement(&create).unwrap();
        let specs = plan_implicit_constraint_indexes(&create, "wr").unwrap();
        let layout = without_rowid_storage_layout("wr", &columns, &specs).unwrap();

        assert_eq!(layout.storage_to_declared, vec![0, 0, 1, 2]);
        assert_eq!(layout.primary_key_declared, vec![0, 0, 1]);
        assert_eq!(
            layout.primary_key_sort_directions,
            vec![SortDirection::Asc, SortDirection::Desc, SortDirection::Asc]
        );
        assert_eq!(
            layout.primary_key_collations,
            vec![Some("NOCASE".to_owned()), Some("BINARY".to_owned()), None]
        );

        let declared = vec![
            SqliteValue::Text("Key".into()),
            SqliteValue::Integer(7),
            SqliteValue::Text("payload".into()),
        ];
        let storage = layout
            .storage_values_from_declared("wr", &declared)
            .unwrap();
        assert_eq!(
            storage,
            vec![
                SqliteValue::Text("Key".into()),
                SqliteValue::Text("Key".into()),
                SqliteValue::Integer(7),
                SqliteValue::Text("payload".into()),
            ]
        );
        assert_eq!(
            layout
                .declared_values_from_storage("wr", &storage, &columns)
                .unwrap(),
            declared
        );

        let inconsistent = vec![
            SqliteValue::Text("Key".into()),
            SqliteValue::Text("Different".into()),
            SqliteValue::Integer(7),
            SqliteValue::Text("payload".into()),
        ];
        let error = layout
            .declared_values_from_storage("wr", &inconsistent, &columns)
            .expect_err("repeated physical copies of one PK column must agree");
        assert!(matches!(error, FrankenError::DatabaseCorrupt { detail }
                if detail.contains("inconsistent copies")
                    && detail.contains("PRIMARY KEY column `key`")));
    }

    #[test]
    fn test_without_rowid_stock_load_rebuild_and_reopen_preserve_physical_layout() {
        asupersync::test_utils::run_test(|| async {
            const TABLE_SQL: &str = "CREATE TABLE wr(
                payload TEXT,
                tenant TEXT COLLATE NOCASE,
                seq INTEGER,
                shard TEXT COLLATE RTRIM,
                note TEXT,
                PRIMARY KEY(shard DESC, tenant ASC),
                UNIQUE(payload COLLATE RTRIM DESC),
                UNIQUE(tenant),
                UNIQUE(tenant COLLATE BINARY)
            ) WITHOUT ROWID";
            const EMPTY_TABLE_SQL: &str = "CREATE TABLE wr_empty(
                body TEXT,
                key_b TEXT COLLATE RTRIM,
                key_a INTEGER,
                PRIMARY KEY(key_b DESC, key_a ASC)
            ) WITHOUT ROWID";
            const IDX_PAYLOAD_SQL: &str =
                "CREATE INDEX idx_payload ON wr(payload COLLATE NOCASE DESC)";
            const IDX_TENANT_BINARY_SQL: &str =
                "CREATE INDEX idx_tenant_binary ON wr(tenant COLLATE BINARY)";
            const IDX_SHARD_ASC_SQL: &str = "CREATE INDEX idx_shard_asc ON wr(shard ASC)";
            const IDX_PARTIAL_SQL: &str =
                "CREATE INDEX idx_partial ON wr(note COLLATE RTRIM ASC) WHERE seq >= 90";

            fn index_xinfo(
                sqlite: &rusqlite::Connection,
                object_name: &str,
            ) -> Vec<(i64, i64, String, i64, String, i64)> {
                let mut statement = sqlite
                    .prepare(
                        "SELECT seqno, cid, name, desc, coll, key
                         FROM pragma_index_xinfo(?1)
                         ORDER BY seqno",
                    )
                    .unwrap();
                statement
                    .query_map([object_name], |row| {
                        Ok((
                            row.get(0)?,
                            row.get(1)?,
                            row.get::<_, Option<String>>(2)?.unwrap_or_default(),
                            row.get(3)?,
                            row.get(4)?,
                            row.get(5)?,
                        ))
                    })
                    .unwrap()
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .unwrap()
            }

            let dir = tempfile::tempdir().unwrap();
            let source_path = dir.path().join("without-rowid-stock-source.db");
            let rebuilt_path = dir.path().join("without-rowid-rebuilt.db");
            {
                let mut sqlite = rusqlite::Connection::open(&source_path).unwrap();
                sqlite
                    .execute_batch(&format!(
                        "{TABLE_SQL};
                         {EMPTY_TABLE_SQL};
                         {IDX_PAYLOAD_SQL};
                         {IDX_TENANT_BINARY_SQL};
                         {IDX_SHARD_ASC_SQL};
                         {IDX_PARTIAL_SQL};"
                    ))
                    .unwrap();
                let transaction = sqlite.transaction().unwrap();
                {
                    let mut insert = transaction
                        .prepare(
                            "INSERT INTO wr(payload, tenant, seq, shard, note)
                             VALUES (?1, ?2, ?3, ?4, ?5)",
                        )
                        .unwrap();
                    for i in 0_i64..180 {
                        insert
                            .execute(rusqlite::params![
                                format!("payload-{i:03}"),
                                format!("Tenant-{i:03}"),
                                i,
                                format!("shard-{:02}  ", i % 17),
                                format!("note-{i:03}-{}", "x".repeat(48)),
                            ])
                            .unwrap();
                    }
                }
                transaction.commit().unwrap();
                assert_eq!(
                    sqlite
                        .query_row::<String, _, _>("PRAGMA integrity_check", [], |row| row.get(0))
                        .unwrap(),
                    "ok"
                );
            }

            let mut loaded = load_test_db(&source_path).await.unwrap();
            let wr_schema = loaded
                .schema
                .iter()
                .find(|table| table.name == "wr")
                .unwrap();
            assert!(wr_schema.without_rowid);
            let wr_root = wr_schema.root_page;
            let loaded_row = loaded
                .db
                .get_table(wr_root)
                .unwrap()
                .iter_rows()
                .find(|(_, values)| values[2] == SqliteValue::Integer(42))
                .map(|(_, values)| values.to_vec())
                .unwrap();
            assert_eq!(loaded_row[0], SqliteValue::Text("payload-042".into()));
            assert_eq!(loaded_row[1], SqliteValue::Text("Tenant-042".into()));
            assert_eq!(loaded_row[3], SqliteValue::Text("shard-08  ".into()));

            // Exercise a FrankenSQLite-side in-memory write before rebuilding.
            loaded.db.get_table_mut(wr_root).unwrap().insert_row(
                10_000,
                vec![
                    SqliteValue::Text("payload-extra".into()),
                    SqliteValue::Text("Tenant-extra".into()),
                    SqliteValue::Integer(10_000),
                    SqliteValue::Text("shard-extra  ".into()),
                    SqliteValue::Text("note-extra".into()),
                ],
            );

            let mut original_ddl = HashMap::new();
            for (name, sql) in [
                ("wr", TABLE_SQL),
                ("wr_empty", EMPTY_TABLE_SQL),
                ("idx_payload", IDX_PAYLOAD_SQL),
                ("idx_tenant_binary", IDX_TENANT_BINARY_SQL),
                ("idx_shard_asc", IDX_SHARD_ASC_SQL),
                ("idx_partial", IDX_PARTIAL_SQL),
            ] {
                original_ddl.insert(name.to_owned(), sql.to_owned());
            }
            let header = DatabaseHeader {
                page_size: DEFAULT_PAGE_SIZE,
                schema_cookie: loaded.schema_cookie,
                change_counter: loaded.change_counter,
                version_valid_for: loaded.change_counter,
                ..DatabaseHeader::default()
            };
            persist_to_sqlite_with_header_and_master_entries(
                &Cx::new(),
                &rebuilt_path,
                &loaded.schema,
                &loaded.db,
                &header,
                &[],
                &original_ddl,
            )
            .await
            .unwrap();

            let sqlite = rusqlite::Connection::open(&rebuilt_path).unwrap();
            assert_eq!(
                sqlite
                    .query_row::<String, _, _>("PRAGMA integrity_check", [], |row| row.get(0))
                    .unwrap(),
                "ok"
            );
            assert_eq!(
                sqlite
                    .query_row::<i64, _, _>("SELECT count(*) FROM wr", [], |row| row.get(0))
                    .unwrap(),
                181
            );
            assert_eq!(
                sqlite
                    .query_row::<i64, _, _>("SELECT count(*) FROM wr_empty", [], |row| row.get(0))
                    .unwrap(),
                0
            );
            for index_name in [
                "idx_payload",
                "idx_tenant_binary",
                "idx_shard_asc",
                "sqlite_autoindex_wr_2",
                "sqlite_autoindex_wr_3",
                "sqlite_autoindex_wr_4",
            ] {
                let query =
                    format!("SELECT count(*) FROM wr INDEXED BY \"{index_name}\" WHERE seq >= 0");
                assert_eq!(
                    sqlite
                        .query_row::<i64, _, _>(&query, [], |row| row.get(0))
                        .unwrap(),
                    181,
                    "{index_name}"
                );
            }
            assert_eq!(
                sqlite
                    .query_row::<i64, _, _>(
                        "SELECT count(*) FROM wr INDEXED BY idx_partial WHERE seq >= 90",
                        [],
                        |row| row.get(0),
                    )
                    .unwrap(),
                91
            );

            assert_eq!(
                index_xinfo(&sqlite, "wr"),
                vec![
                    (0, 3, "shard".to_owned(), 1, "RTRIM".to_owned(), 1),
                    (1, 1, "tenant".to_owned(), 0, "NOCASE".to_owned(), 1),
                    (2, 0, "payload".to_owned(), 0, "BINARY".to_owned(), 0),
                    (3, 2, "seq".to_owned(), 0, "BINARY".to_owned(), 0),
                    (4, 4, "note".to_owned(), 0, "BINARY".to_owned(), 0),
                ]
            );
            assert_eq!(
                index_xinfo(&sqlite, "idx_tenant_binary"),
                vec![
                    (0, 1, "tenant".to_owned(), 0, "BINARY".to_owned(), 1),
                    (1, 3, "shard".to_owned(), 1, "RTRIM".to_owned(), 0),
                    (2, 1, "tenant".to_owned(), 0, "NOCASE".to_owned(), 0),
                ]
            );
            assert_eq!(
                index_xinfo(&sqlite, "idx_shard_asc"),
                vec![
                    (0, 3, "shard".to_owned(), 0, "RTRIM".to_owned(), 1),
                    (1, 1, "tenant".to_owned(), 0, "NOCASE".to_owned(), 0),
                ]
            );
            assert_eq!(
                index_xinfo(&sqlite, "idx_partial"),
                vec![
                    (0, 4, "note".to_owned(), 0, "RTRIM".to_owned(), 1),
                    (1, 3, "shard".to_owned(), 1, "RTRIM".to_owned(), 0),
                    (2, 1, "tenant".to_owned(), 0, "NOCASE".to_owned(), 0),
                ]
            );
            assert_eq!(
                index_xinfo(&sqlite, "sqlite_autoindex_wr_2"),
                vec![
                    (0, 0, "payload".to_owned(), 1, "RTRIM".to_owned(), 1),
                    (1, 3, "shard".to_owned(), 0, "RTRIM".to_owned(), 0),
                    (2, 1, "tenant".to_owned(), 0, "NOCASE".to_owned(), 0),
                ]
            );
            assert_eq!(
                index_xinfo(&sqlite, "sqlite_autoindex_wr_3"),
                vec![
                    (0, 1, "tenant".to_owned(), 0, "NOCASE".to_owned(), 1),
                    (1, 3, "shard".to_owned(), 0, "RTRIM".to_owned(), 0),
                ]
            );
            assert_eq!(
                index_xinfo(&sqlite, "sqlite_autoindex_wr_4"),
                vec![
                    (0, 1, "tenant".to_owned(), 0, "BINARY".to_owned(), 1),
                    (1, 3, "shard".to_owned(), 0, "RTRIM".to_owned(), 0),
                    (2, 1, "tenant".to_owned(), 0, "NOCASE".to_owned(), 0),
                ]
            );

            let empty_root: i64 = sqlite
                .query_row(
                    "SELECT rootpage FROM sqlite_schema
                     WHERE type='table' AND name='wr_empty'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            let page_size: i64 = sqlite
                .query_row("PRAGMA page_size", [], |row| row.get(0))
                .unwrap();
            drop(sqlite);
            let rebuilt_bytes = std::fs::read(&rebuilt_path).unwrap();
            let empty_root_offset = usize::try_from((empty_root - 1) * page_size).unwrap();
            assert_eq!(
                rebuilt_bytes[empty_root_offset], 0x0A,
                "empty WITHOUT ROWID table root must be a leaf-index page"
            );

            let reopened = load_test_db(&rebuilt_path).await.unwrap();
            let wr_schema = reopened
                .schema
                .iter()
                .find(|table| table.name == "wr")
                .unwrap();
            let implicit_roots = wr_schema
                .indexes
                .iter()
                .filter(|index| parse_autoindex_ordinal(&index.name, "wr").is_some())
                .map(|index| (index.name.as_str(), index.root_page))
                .collect::<Vec<_>>();
            assert_eq!(
                implicit_roots
                    .iter()
                    .map(|(name, _)| *name)
                    .collect::<Vec<_>>(),
                vec![
                    "sqlite_autoindex_wr_2",
                    "sqlite_autoindex_wr_3",
                    "sqlite_autoindex_wr_4",
                ]
            );
            assert!(
                implicit_roots
                    .iter()
                    .all(|(_, root)| *root > 0 && *root != wr_schema.root_page),
                "{implicit_roots:?}"
            );
            assert_eq!(
                implicit_roots
                    .iter()
                    .map(|(_, root)| *root)
                    .collect::<HashSet<_>>()
                    .len(),
                implicit_roots.len(),
                "{implicit_roots:?}"
            );
            let rows = reopened
                .db
                .get_table(wr_schema.root_page)
                .unwrap()
                .iter_rows()
                .map(|(_, values)| values.to_vec())
                .collect::<Vec<_>>();
            assert_eq!(rows.len(), 181);
            assert!(rows.iter().any(|values| {
                values[0] == SqliteValue::Text("payload-extra".into())
                    && values[1] == SqliteValue::Text("Tenant-extra".into())
                    && values[2] == SqliteValue::Integer(10_000)
                    && values[3] == SqliteValue::Text("shard-extra  ".into())
            }));
        });
    }

    #[test]
    fn test_without_rowid_expression_index_load_fails_closed() {
        asupersync::test_utils::run_test(|| async {
            const TABLE_SQL: &str =
                "CREATE TABLE wr(payload TEXT, key TEXT PRIMARY KEY) WITHOUT ROWID";
            const INDEX_SQL: &str = "CREATE INDEX idx_expr ON wr(lower(payload))";
            let dir = tempfile::tempdir().unwrap();
            let source_path = dir.path().join("without-rowid-expression-source.db");
            {
                let sqlite = rusqlite::Connection::open(&source_path).unwrap();
                sqlite
                    .execute_batch(&format!(
                        "{TABLE_SQL}; {INDEX_SQL}; INSERT INTO wr VALUES ('Alpha', 'k1');"
                    ))
                    .unwrap();
            }

            let error = load_test_db(&source_path).await.unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("lacks plain-column physical-key provenance"),
                "{error}"
            );
        });
    }

    #[test]
    fn test_without_rowid_invalid_partial_index_export_fails_before_target_creation() {
        asupersync::test_utils::run_test(|| async {
            const TABLE_SQL: &str =
                "CREATE TABLE wr(payload TEXT, key TEXT PRIMARY KEY) WITHOUT ROWID";
            const INDEX_SQL: &str =
                "CREATE INDEX idx_partial ON wr(payload) WHERE payload IS NOT NULL";
            let dir = tempfile::tempdir().unwrap();
            let source_path = dir.path().join("without-rowid-partial-source.db");
            let target_path = dir.path().join("without-rowid-partial-target.db");
            {
                let sqlite = rusqlite::Connection::open(&source_path).unwrap();
                sqlite
                    .execute_batch(&format!(
                        "{TABLE_SQL}; {INDEX_SQL}; INSERT INTO wr VALUES ('Alpha', 'k1');"
                    ))
                    .unwrap();
            }

            let mut loaded = load_test_db(&source_path).await.unwrap();
            loaded
                .schema
                .iter_mut()
                .find(|table| table.name == "wr")
                .unwrap()
                .indexes
                .iter_mut()
                .find(|index| index.name == "idx_partial")
                .unwrap()
                .where_clause = Some("(".to_owned());
            let mut original_ddl = HashMap::new();
            original_ddl.insert("wr".to_owned(), TABLE_SQL.to_owned());
            original_ddl.insert("idx_partial".to_owned(), INDEX_SQL.to_owned());
            let header = DatabaseHeader {
                schema_cookie: loaded.schema_cookie,
                change_counter: loaded.change_counter,
                version_valid_for: loaded.change_counter,
                ..DatabaseHeader::default()
            };
            let error = persist_to_sqlite_with_header_and_master_entries(
                &Cx::new(),
                &target_path,
                &loaded.schema,
                &loaded.db,
                &header,
                &[],
                &original_ddl,
            )
            .await
            .unwrap_err();
            assert!(
                error.to_string().contains("partial index `idx_partial`"),
                "{error}"
            );
            assert!(
                !target_path.exists(),
                "invalid WITHOUT ROWID partial-index metadata must fail before creating the target"
            );
        });
    }

    #[test]
    fn test_parse_virtual_table_columns_from_sql_rejects_trailing_junk() {
        assert!(
            parse_virtual_table_columns_from_sql("CREATE VIRTUAL TABLE docs USING fts5(a) garbage")
                .is_none(),
            "trailing tokens must invalidate virtual-table SQL during compat import"
        );
    }

    #[test]
    fn test_load_sqlite3_created_file() {
        asupersync::test_utils::run_test(|| async {
            let dir = tempfile::tempdir().unwrap();
            let db_path = dir.path().join("from_c.db");

            // Create with C SQLite via rusqlite.
            {
                let conn = rusqlite::Connection::open(&db_path).unwrap();
                conn.execute_batch(
                    "CREATE TABLE items (val INTEGER, label TEXT);
                 INSERT INTO items VALUES (10, 'alpha');
                 INSERT INTO items VALUES (20, 'beta');",
                )
                .unwrap();
            }

            // Load with our compat loader.
            let loaded = load_test_db(&db_path).await.unwrap();
            assert_eq!(loaded.schema.len(), 1);
            assert_eq!(loaded.schema[0].name, "items");

            let table = loaded.db.get_table(loaded.schema[0].root_page).unwrap();
            let rows: Vec<_> = table.iter_rows().collect();
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].1[0], SqliteValue::Integer(10));
            assert_eq!(rows[0].1[1], SqliteValue::Text("alpha".into()));
            assert_eq!(rows[1].1[0], SqliteValue::Integer(20));
            assert_eq!(rows[1].1[1], SqliteValue::Text("beta".into()));
        });
    }

    #[test]
    fn test_load_sqlite3_created_file_restores_integer_primary_key_alias_values() {
        asupersync::test_utils::run_test(|| async {
            let dir = tempfile::tempdir().unwrap();
            let db_path = dir.path().join("from_c_ipk.db");

            {
                let conn = rusqlite::Connection::open(&db_path).unwrap();
                conn.execute_batch(
                    "CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT);
                 INSERT INTO items (id, label) VALUES (10, 'alpha');
                 INSERT INTO items (id, label) VALUES (20, 'beta');",
                )
                .unwrap();
            }

            let loaded = load_test_db(&db_path).await.unwrap();
            assert_eq!(loaded.schema.len(), 1);
            assert_eq!(loaded.schema[0].name, "items");
            assert!(loaded.schema[0].columns[0].is_ipk);
            assert!(
                loaded.schema[0].indexes.is_empty(),
                "table-level INTEGER PRIMARY KEY rowid aliases must not synthesize autoindexes"
            );

            let table = loaded.db.get_table(loaded.schema[0].root_page).unwrap();
            let rows: Vec<_> = table.iter_rows().collect();
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].0, 10);
            assert_eq!(rows[0].1[0], SqliteValue::Integer(10));
            assert_eq!(rows[0].1[1], SqliteValue::Text("alpha".into()));
            assert_eq!(rows[1].0, 20);
            assert_eq!(rows[1].1[0], SqliteValue::Integer(20));
            assert_eq!(rows[1].1[1], SqliteValue::Text("beta".into()));
        });
    }

    #[test]
    fn test_load_sqlite3_created_file_restores_table_level_integer_primary_key_alias_values() {
        asupersync::test_utils::run_test(|| async {
            let dir = tempfile::tempdir().unwrap();
            let db_path = dir.path().join("from_c_table_pk.db");

            {
                let conn = rusqlite::Connection::open(&db_path).unwrap();
                conn.execute_batch(
                    "CREATE TABLE items (id INTEGER, label TEXT, PRIMARY KEY(id));
                 INSERT INTO items (id, label) VALUES (10, 'alpha');
                 INSERT INTO items (id, label) VALUES (20, 'beta');",
                )
                .unwrap();
            }

            let loaded = load_test_db(&db_path).await.unwrap();
            assert_eq!(loaded.schema.len(), 1);
            assert_eq!(loaded.schema[0].name, "items");
            assert!(loaded.schema[0].columns[0].is_ipk);

            let table = loaded.db.get_table(loaded.schema[0].root_page).unwrap();
            let rows: Vec<_> = table.iter_rows().collect();
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].0, 10);
            assert_eq!(rows[0].1[0], SqliteValue::Integer(10));
            assert_eq!(rows[0].1[1], SqliteValue::Text("alpha".into()));
            assert_eq!(rows[1].0, 20);
            assert_eq!(rows[1].1[0], SqliteValue::Integer(20));
            assert_eq!(rows[1].1[1], SqliteValue::Text("beta".into()));
        });
    }

    #[test]
    fn test_load_sqlite3_rowid_alias_multi_alter_short_rows_preserves_alignment() {
        asupersync::test_utils::run_test(|| async {
            let dir = tempfile::tempdir().unwrap();
            let db_path = dir.path().join("from_c_ipk_multi_alter.db");

            {
                let conn = rusqlite::Connection::open(&db_path).unwrap();
                conn.execute_batch(
                    "CREATE TABLE items (
                    prefix TEXT,
                    id INTEGER PRIMARY KEY,
                    nullable TEXT,
                    required TEXT NOT NULL
                 );
                 INSERT INTO items(prefix, id, nullable, required)
                 VALUES ('p', 7, NULL, 'keep');
                 ALTER TABLE items ADD COLUMN extra TEXT DEFAULT 'x';
                 ALTER TABLE items ADD COLUMN note INTEGER DEFAULT 9;",
                )
                .unwrap();
            }

            let loaded = load_test_db(&db_path).await.unwrap();
            assert_eq!(loaded.schema.len(), 1);
            assert_eq!(loaded.schema[0].name, "items");

            let table = loaded.db.get_table(loaded.schema[0].root_page).unwrap();
            let rows: Vec<_> = table.iter_rows().collect();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].0, 7);
            assert_eq!(rows[0].1[0], SqliteValue::Text("p".into()));
            assert_eq!(rows[0].1[1], SqliteValue::Integer(7));
            assert_eq!(rows[0].1[2], SqliteValue::Null);
            assert_eq!(rows[0].1[3], SqliteValue::Text("keep".into()));
            assert_eq!(rows[0].1[4], SqliteValue::Text("x".into()));
            assert_eq!(rows[0].1[5], SqliteValue::Integer(9));
        });
    }

    #[test]
    fn test_load_sqlite3_rowid_alias_parenthesized_added_defaults() {
        asupersync::test_utils::run_test(|| async {
            let dir = tempfile::tempdir().unwrap();
            let db_path = dir.path().join("from_c_ipk_parenthesized_defaults.db");

            {
                let conn = rusqlite::Connection::open(&db_path).unwrap();
                conn.execute_batch(
                    "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT);
                 INSERT INTO items(id, name) VALUES (3, 'alpha');
                 ALTER TABLE items ADD COLUMN score INTEGER DEFAULT (9);
                 ALTER TABLE items ADD COLUMN tag TEXT DEFAULT ('fallback');",
                )
                .unwrap();
            }

            let loaded = load_test_db(&db_path).await.unwrap();
            let table = loaded.db.get_table(loaded.schema[0].root_page).unwrap();
            let rows: Vec<_> = table.iter_rows().collect();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].0, 3);
            assert_eq!(rows[0].1[0], SqliteValue::Integer(3));
            assert_eq!(rows[0].1[1], SqliteValue::Text("alpha".into()));
            assert_eq!(rows[0].1[2], SqliteValue::Integer(9));
            assert_eq!(rows[0].1[3], SqliteValue::Text("fallback".into()));
        });
    }

    #[test]
    fn test_load_sqlite3_altered_short_rows_parse_boolean_blob_and_quoted_defaults() {
        asupersync::test_utils::run_test(|| async {
            let dir = tempfile::tempdir().unwrap();
            let db_path = dir.path().join("from_c_ipk_literal_defaults.db");

            {
                let conn = rusqlite::Connection::open(&db_path).unwrap();
                conn.execute_batch(
                    r#"CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT);
                 INSERT INTO items(id, name) VALUES (5, 'alpha');
                 ALTER TABLE items ADD COLUMN active BOOLEAN DEFAULT TRUE;
                 ALTER TABLE items ADD COLUMN disabled BOOLEAN DEFAULT FALSE;
                 ALTER TABLE items ADD COLUMN payload BLOB DEFAULT X'6162';
                 ALTER TABLE items ADD COLUMN tag TEXT DEFAULT "fallback";"#,
                )
                .unwrap();
            }

            let loaded = load_test_db(&db_path).await.unwrap();
            let table = loaded.db.get_table(loaded.schema[0].root_page).unwrap();
            let rows: Vec<_> = table.iter_rows().collect();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].0, 5);
            assert_eq!(rows[0].1[0], SqliteValue::Integer(5));
            assert_eq!(rows[0].1[1], SqliteValue::Text("alpha".into()));
            assert_eq!(rows[0].1[2], SqliteValue::Integer(1));
            assert_eq!(rows[0].1[3], SqliteValue::Integer(0));
            assert_eq!(rows[0].1[4], SqliteValue::from(vec![0x61, 0x62]));
            assert_eq!(rows[0].1[5], SqliteValue::Text("fallback".into()));
        });
    }

    #[test]
    fn test_inflate_loaded_rowid_alias_omitted_slot_keeps_shifted_null_alignment() {
        let column = |name: &str, affinity: char, is_ipk: bool| ColumnInfo {
            name: name.to_owned(),
            affinity,
            is_ipk,
            type_name: None,
            notnull: false,
            unique: false,
            default_value: None,
            strict_type: None,
            generated_expr: None,
            generated_stored: None,
            collation: None,
            conflict_action: None,
        };
        let mut required = column("required", 'B', false);
        required.notnull = true;
        let mut extra = column("extra", 'B', false);
        extra.default_value = Some("'x'".to_owned());
        let mut note = column("note", 'D', false);
        note.default_value = Some("9".to_owned());
        let columns = vec![
            column("prefix", 'B', false),
            column("id", 'D', true),
            column("nullable", 'B', false),
            required,
            extra,
            note,
        ];
        let mut values = vec![
            SqliteValue::Text("p".into()),
            SqliteValue::Null,
            SqliteValue::Text("keep".into()),
        ];

        inflate_loaded_table_row_values(&mut values, 7, &columns, Some(1), "items").unwrap();

        assert_eq!(values[0], SqliteValue::Text("p".into()));
        assert_eq!(values[1], SqliteValue::Integer(7));
        assert_eq!(values[2], SqliteValue::Null);
        assert_eq!(values[3], SqliteValue::Text("keep".into()));
        assert_eq!(values[4], SqliteValue::Text("x".into()));
        assert_eq!(values[5], SqliteValue::Integer(9));
    }

    #[test]
    fn test_load_sqlite3_created_file_with_nondefault_page_size_and_reserved_bytes() {
        asupersync::test_utils::run_test(|| async {
            if Command::new("sqlite3").arg("--version").output().is_err() {
                eprintln!("skipping: sqlite3 binary not found");
                return;
            }

            let dir = tempfile::tempdir().unwrap();
            let db_path = dir.path().join("from_c_reserved_bytes.db");

            let mut child = Command::new("sqlite3")
                .arg(&db_path)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .expect("sqlite3 process should start");
            {
                let mut stdin = child
                    .stdin
                    .take()
                    .expect("sqlite3 stdin should be available");
                stdin
                    .write_all(
                        br"PRAGMA journal_mode=DELETE;
PRAGMA page_size=8192;
VACUUM;
.filectrl reserve_bytes 32
VACUUM;
CREATE TABLE items (val INTEGER, label TEXT);
INSERT INTO items VALUES (10, 'alpha');
INSERT INTO items VALUES (20, 'beta');
PRAGMA integrity_check;
",
                    )
                    .expect("sqlite3 setup should accept the script");
            }
            let output = child
                .wait_with_output()
                .expect("sqlite3 process should finish");
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            if !output.status.success()
                && (stdout.contains("unknown")
                    || stdout.contains("Usage:")
                    || stderr.contains("unknown")
                    || stderr.contains("Usage:"))
            {
                eprintln!(
                    "skipping: sqlite3 shell does not support .filectrl reserve_bytes: stdout={stdout} stderr={stderr}"
                );
                return;
            }
            assert!(
                output.status.success(),
                "sqlite3 reserved-byte setup failed: stdout={stdout} stderr={stderr}"
            );
            assert!(
                stdout.lines().any(|line| line.trim() == "ok"),
                "sqlite3 should report integrity_check=ok for the reserved-byte database: stdout={stdout} stderr={stderr}"
            );

            let loaded = load_test_db(&db_path).await.unwrap_or_else(|error| {
            panic!(
                "compat loader must read valid C SQLite files with non-default page sizes and reserved bytes: {error}"
            )
        });
            assert_eq!(loaded.schema.len(), 1);
            assert_eq!(loaded.schema[0].name, "items");

            let table = loaded.db.get_table(loaded.schema[0].root_page).unwrap();
            let rows: Vec<_> = table.iter_rows().collect();
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].1[0], SqliteValue::Integer(10));
            assert_eq!(rows[0].1[1], SqliteValue::Text("alpha".into()));
            assert_eq!(rows[1].1[0], SqliteValue::Integer(20));
            assert_eq!(rows[1].1[1], SqliteValue::Text("beta".into()));
        });
    }

    #[test]
    fn test_is_sqlite_format_text_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("text.db");
        host_fs::write(&path, b"CREATE TABLE t (x);").unwrap();
        assert!(!is_sqlite_format(&path));
    }

    #[test]
    fn test_is_sqlite_format_nonexistent() {
        assert!(!is_sqlite_format(Path::new(
            "/tmp/nonexistent_compat_test.db"
        )));
    }

    #[test]
    fn test_multiple_tables_roundtrip() {
        asupersync::test_utils::run_test(|| async {
            let dir = tempfile::tempdir().unwrap();
            let db_path = dir.path().join("multi.db");

            let mut db = MemDatabase::new();
            let root_a = db.create_table(1);
            db.tables
                .get_mut(&root_a)
                .unwrap()
                .insert_row(1, vec![SqliteValue::Text("row_a".into())]);

            let root_b = db.create_table(1);
            db.tables
                .get_mut(&root_b)
                .unwrap()
                .insert_row(1, vec![SqliteValue::Integer(777)]);

            let schema = vec![
                TableSchema {
                    name: "alpha".to_owned(),
                    root_page: root_a,
                    columns: vec![ColumnInfo {
                        name: "val".to_owned(),
                        affinity: 'C',
                        is_ipk: false,
                        type_name: None,
                        notnull: false,
                        unique: false,
                        default_value: None,
                        strict_type: None,
                        generated_expr: None,
                        generated_stored: None,
                        collation: None,
                        conflict_action: None,
                    }],
                    indexes: Vec::new(),
                    strict: false,
                    without_rowid: false,
                    primary_key_constraints: Vec::new(),
                    foreign_keys: Vec::new(),
                    check_constraints: Vec::new(),
                },
                TableSchema {
                    name: "beta".to_owned(),
                    root_page: root_b,
                    columns: vec![ColumnInfo {
                        name: "num".to_owned(),
                        affinity: 'd',
                        is_ipk: false,
                        type_name: None,
                        notnull: false,
                        unique: false,
                        default_value: None,
                        strict_type: None,
                        generated_expr: None,
                        generated_stored: None,
                        collation: None,
                        conflict_action: None,
                    }],
                    indexes: Vec::new(),
                    strict: false,
                    without_rowid: false,
                    primary_key_constraints: Vec::new(),
                    foreign_keys: Vec::new(),
                    check_constraints: Vec::new(),
                },
            ];

            persist_test_db(&db_path, &schema, &db, 0, 0).await.unwrap();
            let loaded = load_test_db(&db_path).await.unwrap();

            assert_eq!(loaded.schema.len(), 2);
            assert_eq!(loaded.schema[0].name, "alpha");
            assert_eq!(loaded.schema[1].name, "beta");

            let tbl_a = loaded.db.get_table(loaded.schema[0].root_page).unwrap();
            let rows_a: Vec<_> = tbl_a.iter_rows().collect();
            assert_eq!(rows_a[0].1[0], SqliteValue::Text("row_a".into()));

            let tbl_b = loaded.db.get_table(loaded.schema[1].root_page).unwrap();
            let rows_b: Vec<_> = tbl_b.iter_rows().collect();
            assert_eq!(rows_b[0].1[0], SqliteValue::Integer(777));
        });
    }

    #[test]
    fn test_parse_columns_from_create_sql() {
        let sql = r#"CREATE TABLE "foo" ("id" INTEGER, "name" TEXT, "data" BLOB)"#;
        let cols = parse_columns_from_create_sql(sql);
        assert_eq!(cols.len(), 3);
        assert_eq!(cols[0].name, "id");
        assert_eq!(cols[0].affinity, 'D');
        assert_eq!(cols[1].name, "name");
        assert_eq!(cols[1].affinity, 'B');
        assert_eq!(cols[2].name, "data");
        assert_eq!(cols[2].affinity, 'A');
    }

    #[test]
    fn test_parse_columns_from_create_sql_handles_nested_commas_and_constraints() {
        let sql = r"CREATE TABLE metrics (
            id INTEGER PRIMARY KEY,
            amount DECIMAL(10,2) NOT NULL,
            status TEXT CHECK (status IN ('a,b', 'c')),
            CONSTRAINT metrics_pk PRIMARY KEY (id)
        )";
        let cols = parse_columns_from_create_sql(sql);
        assert_eq!(cols.len(), 3);
        assert_eq!(cols[0].name, "id");
        assert_eq!(cols[0].affinity, 'D');
        assert!(cols[0].is_ipk);
        assert_eq!(cols[1].name, "amount");
        assert_eq!(cols[1].affinity, 'C');
        assert_eq!(cols[2].name, "status");
        assert_eq!(cols[2].affinity, 'B');
    }

    #[test]
    fn test_parse_columns_from_create_sql_table_level_integer_primary_key_is_ipk() {
        let sql = "CREATE TABLE metrics (id INTEGER, body TEXT, PRIMARY KEY(id))";
        let cols = parse_columns_from_create_sql(sql);
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].name, "id");
        assert!(cols[0].is_ipk);
        assert_eq!(cols[1].name, "body");
    }

    #[test]
    fn test_parse_columns_distinguishes_column_and_table_unique_ownership() {
        let column_owned = parse_columns_from_create_sql(
            "CREATE TABLE column_owned (id INTEGER UNIQUE, body TEXT)",
        );
        assert!(column_owned[0].unique);

        let table_owned = parse_columns_from_create_sql(
            "CREATE TABLE table_owned (id INTEGER, body TEXT, UNIQUE(id))",
        );
        assert!(!table_owned[0].unique);
        let indexes = extract_unique_constraint_indexes_from_sql(
            "CREATE TABLE table_owned (id INTEGER, body TEXT, UNIQUE(id))",
            "table_owned",
        )
        .unwrap();
        assert_eq!(indexes.len(), 1);
        assert_eq!(indexes[0].columns, vec!["id"]);
    }

    #[test]
    fn test_parse_columns_from_create_sql_table_level_integer_primary_key_desc_is_ipk() {
        let sql = "CREATE TABLE metrics (id INTEGER, body TEXT, PRIMARY KEY(id DESC))";
        let cols = parse_columns_from_create_sql(sql);
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].name, "id");
        assert!(cols[0].is_ipk);
        assert_eq!(cols[1].name, "body");
    }

    #[test]
    fn test_parse_columns_from_create_sql_table_level_integer_primary_key_collate_desc_is_ipk() {
        let sql =
            "CREATE TABLE metrics (id INTEGER, body TEXT, PRIMARY KEY(id COLLATE NOCASE DESC))";
        let cols = parse_columns_from_create_sql(sql);
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].name, "id");
        assert!(cols[0].is_ipk);
        assert_eq!(cols[1].name, "body");
    }

    #[test]
    fn test_parse_columns_from_create_sql_without_rowid_integer_pk_is_not_ipk() {
        let sql = "CREATE TABLE wr (id INTEGER PRIMARY KEY, body TEXT) WITHOUT ROWID";
        let cols = parse_columns_from_create_sql(sql);
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].name, "id");
        assert!(!cols[0].is_ipk);
        assert!(cols[0].unique);
        assert_eq!(cols[1].name, "body");
    }

    #[test]
    fn test_parse_columns_from_create_sql_keeps_quoted_keyword_column_name() {
        let sql = r#"CREATE TABLE t ("primary" TEXT, value INTEGER)"#;
        let cols = parse_columns_from_create_sql(sql);
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].name, "primary");
        assert_eq!(cols[0].affinity, 'B');
        assert_eq!(cols[1].name, "value");
        assert_eq!(cols[1].affinity, 'D');
    }

    #[test]
    fn test_parse_columns_from_create_sql_handles_quoted_names_with_spaces() {
        let sql = r#"CREATE TABLE t ("first name" TEXT, [last name] INTEGER, `role name` NUMERIC)"#;
        let cols = parse_columns_from_create_sql(sql);
        assert_eq!(cols.len(), 3);
        assert_eq!(cols[0].name, "first name");
        assert_eq!(cols[0].affinity, 'B');
        assert_eq!(cols[1].name, "last name");
        assert_eq!(cols[1].affinity, 'D');
        assert_eq!(cols[2].name, "role name");
        assert_eq!(cols[2].affinity, 'C');
    }

    #[test]
    fn test_parse_columns_from_create_sql_ignores_constraint_keywords_inside_default_literals() {
        let sql = r#"CREATE TABLE t (
            note TEXT DEFAULT 'NOT NULL UNIQUE PRIMARY KEY',
            tag TEXT DEFAULT "fallback"
        )"#;
        let cols = parse_columns_from_create_sql(sql);
        assert_eq!(cols.len(), 2);
        assert!(!cols[0].notnull);
        assert!(!cols[0].unique);
        assert!(!cols[0].is_ipk);
        assert_eq!(
            cols[0].default_value.as_deref(),
            Some("'NOT NULL UNIQUE PRIMARY KEY'")
        );
        assert_eq!(cols[1].default_value.as_deref(), Some("fallback"));
    }

    #[test]
    fn test_parse_columns_fallback_ignores_constraint_keywords_inside_default_literals() {
        let sql = r#"CREATE TABLE t (
            note TEXT DEFAULT 'NOT NULL UNIQUE PRIMARY KEY COLLATE bogus',
            actual INTEGER DEFAULT "PRIMARY KEY" PRIMARY KEY,
            required TEXT DEFAULT "UNIQUE" NOT NULL,
            uniq TEXT DEFAULT "NOT NULL" UNIQUE COLLATE nocase
        ) trailing"#;
        let cols = parse_columns_from_create_sql(sql);

        assert_eq!(cols.len(), 4);
        assert!(!cols[0].notnull);
        assert!(!cols[0].unique);
        assert!(!cols[0].is_ipk);
        assert_eq!(cols[0].collation, None);
        assert!(cols[1].is_ipk);
        assert!(cols[1].unique);
        assert!(cols[2].notnull);
        assert!(!cols[2].unique);
        assert!(!cols[3].notnull);
        assert!(cols[3].unique);
        assert_eq!(cols[3].collation.as_deref(), Some("NOCASE"));
    }

    #[test]
    fn test_parse_columns_fallback_finds_unquoted_default_keyword() {
        let sql = r#"CREATE TABLE t (
            note TEXT CHECK (note <> 'DEFAULT NOT NULL') DEFAULT 'ok',
            other TEXT CHECK (other <> "DEFAULT UNIQUE")
        ) trailing"#;
        let cols = parse_columns_from_create_sql(sql);

        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].default_value.as_deref(), Some("'ok'"));
        assert!(!cols[0].notnull);
        assert_eq!(cols[1].default_value, None);
        assert!(!cols[1].unique);
    }

    #[test]
    fn test_parse_columns_fallback_preserves_generated_column_metadata() {
        let sql = "CREATE TABLE t(a, c, b GENERATED ALWAYS AS(a * 2) STORED, CHECK(c > 0) ON CONFLICT FAIL)";
        let cols = parse_columns_from_create_sql(sql);

        assert_eq!(cols.len(), 3);
        assert_eq!(cols[2].name, "b");
        assert_eq!(cols[2].generated_expr.as_deref(), Some("a * 2"));
        assert_eq!(cols[2].generated_stored, Some(true));

        let virtual_cols = parse_columns_from_create_sql(
            "CREATE TABLE v(a, b GENERATED ALWAYS AS(a) REFERENCES stored, CHECK(a > 0) ON CONFLICT FAIL)",
        );
        assert_eq!(virtual_cols[1].generated_expr.as_deref(), Some("a"));
        assert_eq!(virtual_cols[1].generated_stored, Some(false));
    }

    #[test]
    fn test_parse_columns_fallback_keeps_quoted_collation_names() {
        let sql = r#"CREATE TABLE t (
            name TEXT COLLATE "NOCASE",
            code TEXT COLLATE [RTRIM],
            note TEXT COLLATE 'BINARY',
            tag/* name/type comment, comma */TEXT COLLATE/* collation comment, comma */`NOCASE`
        ) trailing"#;
        let cols = parse_columns_from_create_sql(sql);

        assert_eq!(cols.len(), 4);
        assert_eq!(cols[0].collation.as_deref(), Some("NOCASE"));
        assert_eq!(cols[1].collation.as_deref(), Some("RTRIM"));
        assert_eq!(cols[2].collation.as_deref(), Some("BINARY"));
        assert_eq!(cols[3].collation.as_deref(), Some("NOCASE"));
    }

    #[test]
    fn test_parse_columns_from_create_sql_preserves_type_arguments() {
        let sql = "CREATE TABLE metrics (amount DECIMAL(10, 2), name VARCHAR(255))";
        let cols = parse_columns_from_create_sql(sql);
        assert_eq!(cols[0].type_name.as_deref(), Some("DECIMAL(10, 2)"));
        assert_eq!(cols[1].type_name.as_deref(), Some("VARCHAR(255)"));
    }

    #[test]
    fn test_parse_columns_from_beads_style_multiline_create_table_sql() {
        let cases = [
            (
                "labels",
                r"CREATE TABLE labels (
                    issue_id TEXT NOT NULL,
                    label TEXT NOT NULL,
                    PRIMARY KEY (issue_id, label),
                    FOREIGN KEY (issue_id) REFERENCES issues(id) ON DELETE CASCADE
                )",
                &["issue_id", "label"][..],
            ),
            (
                "comments",
                r"CREATE TABLE comments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    issue_id TEXT NOT NULL,
                    author TEXT NOT NULL,
                    text TEXT NOT NULL,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (issue_id) REFERENCES issues(id) ON DELETE CASCADE
                )",
                &["id", "issue_id", "author", "text", "created_at"][..],
            ),
            (
                "events",
                r"CREATE TABLE events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    issue_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    actor TEXT NOT NULL DEFAULT '',
                    old_value TEXT,
                    new_value TEXT,
                    comment TEXT,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (issue_id) REFERENCES issues(id) ON DELETE CASCADE
                )",
                &[
                    "id",
                    "issue_id",
                    "event_type",
                    "actor",
                    "old_value",
                    "new_value",
                    "comment",
                    "created_at",
                ][..],
            ),
            (
                "config",
                r"CREATE TABLE config (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )",
                &["key", "value"][..],
            ),
            (
                "blocked_issues_cache",
                r"CREATE TABLE blocked_issues_cache (
                    issue_id TEXT PRIMARY KEY,
                    blocked_by TEXT NOT NULL,  -- JSON array of blocking issue IDs
                    blocked_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (issue_id) REFERENCES issues(id) ON DELETE CASCADE
                )",
                &["issue_id", "blocked_by", "blocked_at"][..],
            ),
            (
                "issues",
                r"CREATE TABLE issues (
                    id TEXT PRIMARY KEY,
                    content_hash TEXT,
                    title TEXT NOT NULL,
                    description TEXT NOT NULL DEFAULT '',
                    design TEXT NOT NULL DEFAULT '',
                    acceptance_criteria TEXT NOT NULL DEFAULT '',
                    notes TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT 'open',
                    priority INTEGER NOT NULL DEFAULT 2,
                    issue_type TEXT NOT NULL DEFAULT 'task',
                    assignee TEXT,
                    owner TEXT DEFAULT '',
                    estimated_minutes INTEGER,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    created_by TEXT DEFAULT '',
                    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    closed_at DATETIME,
                    close_reason TEXT DEFAULT '',
                    closed_by_session TEXT DEFAULT '',
                    due_at DATETIME,
                    defer_until DATETIME,
                    external_ref TEXT,
                    source_system TEXT DEFAULT '',
                    source_repo TEXT NOT NULL DEFAULT '.',
                    deleted_at DATETIME,
                    deleted_by TEXT DEFAULT '',
                    delete_reason TEXT DEFAULT '',
                    original_type TEXT DEFAULT '',
                    compaction_level INTEGER DEFAULT 0,
                    compacted_at DATETIME,
                    compacted_at_commit TEXT,
                    original_size INTEGER,
                    sender TEXT DEFAULT '',
                    ephemeral INTEGER DEFAULT 0,
                    pinned INTEGER DEFAULT 0,
                    is_template INTEGER DEFAULT 0,
                    CHECK(length(title) <= 500),
                    CHECK(priority >= 0 AND priority <= 4),
                    CHECK((status = 'closed' AND closed_at IS NOT NULL) OR (status != 'closed'))
                )",
                &[
                    "id",
                    "content_hash",
                    "title",
                    "description",
                    "design",
                    "acceptance_criteria",
                    "notes",
                    "status",
                    "priority",
                    "issue_type",
                    "assignee",
                    "owner",
                    "estimated_minutes",
                    "created_at",
                    "created_by",
                    "updated_at",
                    "closed_at",
                    "close_reason",
                    "closed_by_session",
                    "due_at",
                    "defer_until",
                    "external_ref",
                    "source_system",
                    "source_repo",
                    "deleted_at",
                    "deleted_by",
                    "delete_reason",
                    "original_type",
                    "compaction_level",
                    "compacted_at",
                    "compacted_at_commit",
                    "original_size",
                    "sender",
                    "ephemeral",
                    "pinned",
                    "is_template",
                ][..],
            ),
        ];

        for (table_name, sql, expected_columns) in cases {
            let cols = parse_columns_from_create_sql(sql);
            let actual_names: Vec<&str> = cols.iter().map(|col| col.name.as_str()).collect();
            assert_eq!(
                actual_names, expected_columns,
                "failed to parse Beads-style column list for table {table_name}"
            );
        }
    }

    #[test]
    fn test_build_create_table_sql_appends_strict_keyword() {
        let table = TableSchema {
            name: "strict_table".to_owned(),
            root_page: 2,
            columns: vec![ColumnInfo {
                name: "id".to_owned(),
                affinity: 'D',
                is_ipk: false,
                type_name: Some("INTEGER".to_owned()),
                notnull: false,
                unique: false,
                default_value: None,
                strict_type: Some(StrictColumnType::Integer),
                generated_expr: None,
                generated_stored: None,
                collation: None,
                conflict_action: None,
            }],
            indexes: Vec::new(),
            strict: true,
            without_rowid: false,
            primary_key_constraints: Vec::new(),
            foreign_keys: Vec::new(),
            check_constraints: Vec::new(),
        };

        let sql = build_create_table_sql(&table);
        assert!(
            sql.ends_with(" STRICT"),
            "STRICT tables must round-trip with STRICT suffix: {sql}"
        );
    }

    #[test]
    fn test_build_create_table_sql_preserves_declared_type_text() {
        let table = TableSchema {
            name: "typed_table".to_owned(),
            root_page: 2,
            columns: vec![
                ColumnInfo {
                    name: "amount".to_owned(),
                    affinity: 'C',
                    is_ipk: false,
                    type_name: Some("DECIMAL(10, 2)".to_owned()),
                    notnull: false,
                    unique: false,
                    default_value: None,
                    strict_type: None,
                    generated_expr: None,
                    generated_stored: None,
                    collation: None,
                    conflict_action: None,
                },
                ColumnInfo {
                    name: "name".to_owned(),
                    affinity: 'B',
                    is_ipk: false,
                    type_name: Some("VARCHAR(255)".to_owned()),
                    notnull: false,
                    unique: false,
                    default_value: None,
                    strict_type: None,
                    generated_expr: None,
                    generated_stored: None,
                    collation: None,
                    conflict_action: None,
                },
            ],
            indexes: Vec::new(),
            strict: false,
            without_rowid: false,
            primary_key_constraints: Vec::new(),
            foreign_keys: Vec::new(),
            check_constraints: Vec::new(),
        };

        let sql = build_create_table_sql(&table);
        assert!(sql.contains("\"amount\" DECIMAL(10, 2)"), "{sql}");
        assert!(sql.contains("\"name\" VARCHAR(255)"), "{sql}");
    }

    #[test]
    fn test_build_create_table_sql_preserves_typeless_columns() {
        let table = TableSchema {
            name: "typeless_table".to_owned(),
            root_page: 2,
            columns: vec![ColumnInfo {
                name: "payload".to_owned(),
                affinity: 'A',
                is_ipk: false,
                type_name: None,
                notnull: false,
                unique: false,
                default_value: None,
                strict_type: None,
                generated_expr: None,
                generated_stored: None,
                collation: None,
                conflict_action: None,
            }],
            indexes: Vec::new(),
            strict: false,
            without_rowid: false,
            primary_key_constraints: Vec::new(),
            foreign_keys: Vec::new(),
            check_constraints: Vec::new(),
        };

        let sql = build_create_table_sql(&table);
        assert_eq!(sql, "CREATE TABLE \"typeless_table\" (\"payload\")");
    }

    #[test]
    fn test_build_create_table_sql_escapes_embedded_quotes_in_identifiers() {
        let table = TableSchema {
            name: "ty\"ped_table".to_owned(),
            root_page: 2,
            columns: vec![
                ColumnInfo {
                    name: "pay\"load".to_owned(),
                    affinity: 'A',
                    is_ipk: false,
                    type_name: None,
                    notnull: false,
                    unique: false,
                    default_value: None,
                    strict_type: None,
                    generated_expr: None,
                    generated_stored: None,
                    collation: Some("noca\"se".to_owned()),
                    conflict_action: None,
                },
                ColumnInfo {
                    name: "parent\"id".to_owned(),
                    affinity: 'D',
                    is_ipk: false,
                    type_name: Some("INTEGER".to_owned()),
                    notnull: false,
                    unique: false,
                    default_value: None,
                    strict_type: None,
                    generated_expr: None,
                    generated_stored: None,
                    collation: None,
                    conflict_action: None,
                },
            ],
            indexes: Vec::new(),
            strict: false,
            without_rowid: false,
            primary_key_constraints: Vec::new(),
            foreign_keys: vec![FkDef {
                child_columns: vec![1],
                owner_column: None,
                parent_table: "pa\"rent".to_owned(),
                parent_columns: vec!["id\"x".to_owned()],
                on_delete: FkActionType::Cascade,
                on_update: FkActionType::NoAction,
                deferred: false,
            }],
            check_constraints: Vec::new(),
        };

        let sql = build_create_table_sql(&table);
        assert!(sql.contains("\"ty\"\"ped_table\""), "{sql}");
        assert!(
            sql.contains("\"pay\"\"load\" COLLATE \"noca\"\"se\""),
            "{sql}"
        );
        assert!(
            sql.contains("FOREIGN KEY(\"parent\"\"id\") REFERENCES \"pa\"\"rent\"(\"id\"\"x\")"),
            "{sql}"
        );
    }

    #[test]
    fn test_build_create_table_sql_preserves_primary_key_constraints() {
        let table = TableSchema {
            name: "pk_table".to_owned(),
            root_page: 2,
            columns: vec![
                ColumnInfo {
                    name: "id".to_owned(),
                    affinity: 'B',
                    is_ipk: false,
                    type_name: Some("TEXT".to_owned()),
                    notnull: false,
                    unique: true,
                    default_value: None,
                    strict_type: None,
                    generated_expr: None,
                    generated_stored: None,
                    collation: None,
                    conflict_action: None,
                },
                ColumnInfo {
                    name: "body".to_owned(),
                    affinity: 'A',
                    is_ipk: false,
                    type_name: None,
                    notnull: false,
                    unique: false,
                    default_value: None,
                    strict_type: None,
                    generated_expr: None,
                    generated_stored: None,
                    collation: None,
                    conflict_action: None,
                },
            ],
            indexes: Vec::new(),
            strict: false,
            without_rowid: false,
            primary_key_constraints: vec![PrimaryKeyConstraint::new(
                vec!["id".to_owned()],
                vec![SortDirection::Asc],
                vec![None],
            )],
            foreign_keys: Vec::new(),
            check_constraints: Vec::new(),
        };

        let sql = build_create_table_sql(&table);
        assert!(sql.contains("PRIMARY KEY"), "{sql}");
        assert!(!sql.contains("UNIQUE"), "{sql}");
        assert_eq!(
            sql,
            "CREATE TABLE \"pk_table\" (\"id\" TEXT, \"body\", PRIMARY KEY (\"id\"))"
        );
    }

    #[test]
    fn test_build_create_table_sql_appends_without_rowid_and_strict_options() {
        let table = TableSchema {
            name: "wr_strict".to_owned(),
            root_page: 2,
            columns: vec![ColumnInfo {
                name: "id".to_owned(),
                affinity: 'D',
                is_ipk: false,
                type_name: Some("INTEGER".to_owned()),
                notnull: false,
                unique: true,
                default_value: None,
                strict_type: Some(StrictColumnType::Integer),
                generated_expr: None,
                generated_stored: None,
                collation: None,
                conflict_action: None,
            }],
            indexes: Vec::new(),
            strict: true,
            without_rowid: true,
            primary_key_constraints: Vec::new(),
            foreign_keys: Vec::new(),
            check_constraints: Vec::new(),
        };

        let sql = build_create_table_sql(&table);
        assert!(sql.ends_with(" WITHOUT ROWID, STRICT"), "{sql}");
    }

    #[test]
    fn test_build_create_table_sql_preserves_unique_foreign_key_and_check_constraints() {
        let table = TableSchema {
            name: "child".to_owned(),
            root_page: 2,
            columns: vec![
                ColumnInfo {
                    name: "parent_id".to_owned(),
                    affinity: 'D',
                    is_ipk: false,
                    type_name: Some("INTEGER".to_owned()),
                    notnull: true,
                    unique: false,
                    default_value: None,
                    strict_type: None,
                    generated_expr: None,
                    generated_stored: None,
                    collation: None,
                    conflict_action: None,
                },
                ColumnInfo {
                    name: "slug".to_owned(),
                    affinity: 'B',
                    is_ipk: false,
                    type_name: Some("TEXT".to_owned()),
                    notnull: false,
                    unique: false,
                    default_value: None,
                    strict_type: None,
                    generated_expr: None,
                    generated_stored: None,
                    collation: None,
                    conflict_action: None,
                },
            ],
            indexes: vec![IndexSchema {
                name: "sqlite_autoindex_child_1".to_owned(),
                root_page: 0,
                columns: vec!["parent_id".to_owned(), "slug".to_owned()],
                key_expressions: Vec::new(),
                key_sort_directions: vec![SortDirection::Asc, SortDirection::Asc],
                where_clause: None,
                is_unique: true,
                key_collations: vec![],
                conflict_action: None,
            }],
            strict: false,
            without_rowid: false,
            primary_key_constraints: Vec::new(),
            foreign_keys: vec![FkDef {
                child_columns: vec![0],
                owner_column: None,
                parent_table: "parent".to_owned(),
                parent_columns: vec!["id".to_owned()],
                on_delete: FkActionType::Cascade,
                on_update: FkActionType::Restrict,
                deferred: false,
            }],
            check_constraints: vec![CheckConstraint {
                expr: "length(slug) > 0".to_owned(),
                owner_column: None,
            }],
        };

        let sql = build_create_table_sql(&table);
        assert!(sql.contains("UNIQUE (\"parent_id\", \"slug\")"), "{sql}");
        assert!(
            sql.contains(
                "FOREIGN KEY(\"parent_id\") REFERENCES \"parent\"(\"id\") ON DELETE CASCADE ON UPDATE RESTRICT"
            ),
            "{sql}"
        );
        assert!(sql.contains("CHECK(length(slug) > 0)"), "{sql}");
    }

    #[test]
    fn test_extract_unique_constraint_indexes_from_sql_preserves_table_level_unique_constraints() {
        let indexes = extract_unique_constraint_indexes_from_sql(
            "CREATE TABLE child (tenant TEXT, slug TEXT, UNIQUE(tenant, slug))",
            "child",
        )
        .unwrap();
        assert_eq!(indexes.len(), 1);
        assert_eq!(indexes[0].columns, vec!["tenant", "slug"]);
        assert!(indexes[0].is_unique);
    }

    #[test]
    fn test_extract_unique_constraint_indexes_skips_table_level_integer_primary_key_alias() {
        let indexes = extract_unique_constraint_indexes_from_sql(
            "CREATE TABLE metrics (id INTEGER, body TEXT, PRIMARY KEY(id COLLATE NOCASE DESC))",
            "metrics",
        )
        .unwrap();
        assert!(indexes.is_empty(), "{indexes:?}");
    }

    fn plan_test_implicit_indexes(sql: &str, table_name: &str) -> Vec<ImplicitConstraintIndexSpec> {
        let Some(Statement::CreateTable(create)) = parse_single_statement(sql) else {
            panic!("test CREATE TABLE must parse: {sql}");
        };
        plan_implicit_constraint_indexes(&create, table_name).unwrap()
    }

    #[test]
    fn test_implicit_index_plan_preserves_declaration_order_and_sparse_wr_ordinals() {
        let sql = "CREATE TABLE wr (
            id TEXT PRIMARY KEY,
            tenant TEXT UNIQUE,
            slug TEXT,
            UNIQUE(tenant, slug)
        ) WITHOUT ROWID";
        let specs = plan_test_implicit_indexes(sql, "wr");

        assert_eq!(specs.len(), 3);
        assert_eq!(
            specs
                .iter()
                .map(|spec| (spec.ordinal, spec.name.as_str()))
                .collect::<Vec<_>>(),
            vec![
                (1, "sqlite_autoindex_wr_1"),
                (2, "sqlite_autoindex_wr_2"),
                (3, "sqlite_autoindex_wr_3"),
            ]
        );
        assert_eq!(
            specs.iter().map(|spec| spec.backing).collect::<Vec<_>>(),
            vec![
                ImplicitConstraintIndexBacking::WithoutRowidTableRoot,
                ImplicitConstraintIndexBacking::AuxiliaryBtree,
                ImplicitConstraintIndexBacking::AuxiliaryBtree,
            ]
        );
        assert_eq!(specs[0].columns, vec!["id"]);
        assert_eq!(specs[1].columns, vec!["tenant"]);
        assert_eq!(specs[2].columns, vec!["tenant", "slug"]);

        let auxiliary = extract_unique_constraint_indexes_from_sql(sql, "wr").unwrap();
        assert_eq!(
            auxiliary
                .iter()
                .map(|index| index.name.as_str())
                .collect::<Vec<_>>(),
            vec!["sqlite_autoindex_wr_2", "sqlite_autoindex_wr_3"]
        );
    }

    #[test]
    fn test_implicit_index_plan_matches_hfdt_without_rowid_repro() {
        let sql =
            "CREATE TABLE wr(id TEXT PRIMARY KEY, value TEXT, UNIQUE(id,value)) WITHOUT ROWID";
        let specs = plan_test_implicit_indexes(sql, "wr");

        assert_eq!(specs.len(), 2);
        assert_eq!(
            specs[0].backing,
            ImplicitConstraintIndexBacking::WithoutRowidTableRoot
        );
        assert_eq!(specs[0].name, "sqlite_autoindex_wr_1");
        assert_eq!(specs[0].columns, vec!["id"]);
        assert_eq!(
            specs[1].backing,
            ImplicitConstraintIndexBacking::AuxiliaryBtree
        );
        assert_eq!(specs[1].name, "sqlite_autoindex_wr_2");
        assert_eq!(specs[1].columns, vec!["id", "value"]);

        let auxiliary = extract_unique_constraint_indexes_from_sql(sql, "wr").unwrap();
        assert_eq!(auxiliary.len(), 1);
        assert_eq!(auxiliary[0].name, "sqlite_autoindex_wr_2");
        assert_eq!(auxiliary[0].columns, vec!["id", "value"]);
    }

    #[test]
    fn test_implicit_index_plan_defers_column_integer_primary_key() {
        let cases = [
            (
                "CREATE TABLE rowid_t(u TEXT UNIQUE, id INTEGER PRIMARY KEY, v TEXT UNIQUE)",
                false,
                vec![
                    (
                        "sqlite_autoindex_rowid_t_1",
                        ImplicitConstraintIndexBacking::AuxiliaryBtree,
                        "u",
                    ),
                    (
                        "sqlite_autoindex_rowid_t_2",
                        ImplicitConstraintIndexBacking::AuxiliaryBtree,
                        "v",
                    ),
                ],
            ),
            (
                "CREATE TABLE wr(u TEXT UNIQUE, id INTEGER PRIMARY KEY, v TEXT UNIQUE) WITHOUT ROWID",
                true,
                vec![
                    (
                        "sqlite_autoindex_wr_1",
                        ImplicitConstraintIndexBacking::AuxiliaryBtree,
                        "u",
                    ),
                    (
                        "sqlite_autoindex_wr_2",
                        ImplicitConstraintIndexBacking::AuxiliaryBtree,
                        "v",
                    ),
                    (
                        "sqlite_autoindex_wr_3",
                        ImplicitConstraintIndexBacking::WithoutRowidTableRoot,
                        "id",
                    ),
                ],
            ),
        ];

        for (sql, without_rowid, expected) in cases {
            let table_name = if without_rowid { "wr" } else { "rowid_t" };
            let specs = plan_test_implicit_indexes(sql, table_name);
            assert_eq!(specs.len(), expected.len(), "{sql}: {specs:?}");
            for (spec, (name, backing, column)) in specs.iter().zip(expected) {
                assert_eq!(spec.name, name, "{sql}");
                assert_eq!(spec.backing, backing, "{sql}");
                assert_eq!(spec.columns, vec![column], "{sql}");
            }
        }

        let rowid_asc = plan_test_implicit_indexes(
            "CREATE TABLE rowid_t(id INTEGER PRIMARY KEY ASC, v TEXT UNIQUE)",
            "rowid_t",
        );
        assert_eq!(rowid_asc.len(), 1);
        assert_eq!(rowid_asc[0].columns, vec!["v"]);

        let wr_asc = plan_test_implicit_indexes(
            "CREATE TABLE wr(id INTEGER PRIMARY KEY ASC, v TEXT UNIQUE) WITHOUT ROWID",
            "wr",
        );
        assert_eq!(wr_asc.len(), 2);
        assert_eq!(wr_asc[0].name, "sqlite_autoindex_wr_1");
        assert_eq!(wr_asc[0].columns, vec!["v"]);
        assert_eq!(wr_asc[1].name, "sqlite_autoindex_wr_2");
        assert_eq!(wr_asc[1].columns, vec!["id"]);
        assert_eq!(
            wr_asc[1].backing,
            ImplicitConstraintIndexBacking::WithoutRowidTableRoot
        );
    }

    #[test]
    fn test_implicit_index_plan_defers_table_integer_primary_key_even_desc() {
        let rowid_sql = "CREATE TABLE rowid_t(id INTEGER, v TEXT UNIQUE, PRIMARY KEY(id DESC))";
        let rowid_specs = plan_test_implicit_indexes(rowid_sql, "rowid_t");
        assert_eq!(rowid_specs.len(), 1);
        assert_eq!(rowid_specs[0].name, "sqlite_autoindex_rowid_t_1");
        assert_eq!(rowid_specs[0].columns, vec!["v"]);

        let wr_sql =
            "CREATE TABLE wr(id INTEGER, v TEXT UNIQUE, PRIMARY KEY(id DESC)) WITHOUT ROWID";
        let wr_specs = plan_test_implicit_indexes(wr_sql, "wr");
        assert_eq!(wr_specs.len(), 2);
        assert_eq!(wr_specs[0].name, "sqlite_autoindex_wr_1");
        assert_eq!(wr_specs[0].columns, vec!["v"]);
        assert_eq!(
            wr_specs[0].backing,
            ImplicitConstraintIndexBacking::AuxiliaryBtree
        );
        assert_eq!(wr_specs[1].name, "sqlite_autoindex_wr_2");
        assert_eq!(wr_specs[1].columns, vec!["id"]);
        assert_eq!(
            wr_specs[1].backing,
            ImplicitConstraintIndexBacking::WithoutRowidTableRoot
        );
        assert_eq!(wr_specs[1].key_sort_directions, vec![SortDirection::Desc]);
    }

    #[test]
    fn test_implicit_index_plan_does_not_defer_column_integer_primary_key_desc() {
        let rowid_sql = "CREATE TABLE rowid_t(id INTEGER PRIMARY KEY DESC, v TEXT UNIQUE)";
        let rowid_specs = plan_test_implicit_indexes(rowid_sql, "rowid_t");
        assert_eq!(rowid_specs.len(), 2);
        assert_eq!(rowid_specs[0].columns, vec!["id"]);
        assert_eq!(
            rowid_specs[0].key_sort_directions,
            vec![SortDirection::Desc]
        );
        assert_eq!(rowid_specs[1].columns, vec!["v"]);

        let wr_sql = "CREATE TABLE wr(id INTEGER PRIMARY KEY DESC, v TEXT UNIQUE) WITHOUT ROWID";
        let wr_specs = plan_test_implicit_indexes(wr_sql, "wr");
        assert_eq!(wr_specs.len(), 2);
        assert_eq!(wr_specs[0].name, "sqlite_autoindex_wr_1");
        assert_eq!(
            wr_specs[0].backing,
            ImplicitConstraintIndexBacking::WithoutRowidTableRoot
        );
        assert_eq!(wr_specs[1].name, "sqlite_autoindex_wr_2");
        assert_eq!(
            wr_specs[1].backing,
            ImplicitConstraintIndexBacking::AuxiliaryBtree
        );
    }

    #[test]
    fn test_implicit_index_plan_requires_exact_integer_type_for_deferred_primary_key() {
        let specs =
            plan_test_implicit_indexes("CREATE TABLE t(id INT PRIMARY KEY, v TEXT UNIQUE)", "t");
        assert_eq!(specs.len(), 2);
        assert_eq!(specs[0].columns, vec!["id"]);
        assert_eq!(specs[0].name, "sqlite_autoindex_t_1");
        assert_eq!(specs[1].columns, vec!["v"]);
        assert_eq!(specs[1].name, "sqlite_autoindex_t_2");
    }

    #[test]
    fn test_implicit_index_plan_deferred_pk_promotes_existing_wr_slot() {
        let sql = "CREATE TABLE wr(id INTEGER UNIQUE PRIMARY KEY, v TEXT UNIQUE) WITHOUT ROWID";
        let specs = plan_test_implicit_indexes(sql, "wr");

        assert_eq!(specs.len(), 2);
        assert_eq!(specs[0].name, "sqlite_autoindex_wr_1");
        assert_eq!(specs[0].columns, vec!["id"]);
        assert!(specs[0].is_primary_key);
        assert_eq!(
            specs[0].backing,
            ImplicitConstraintIndexBacking::WithoutRowidTableRoot
        );
        assert_eq!(specs[1].name, "sqlite_autoindex_wr_2");
        assert_eq!(specs[1].columns, vec!["v"]);
        assert_eq!(
            specs[1].backing,
            ImplicitConstraintIndexBacking::AuxiliaryBtree
        );
    }

    #[test]
    fn test_implicit_index_plan_deduplicates_case_direction_and_binary_collation() {
        let specs = plan_test_implicit_indexes(
            "CREATE TABLE t(a TEXT, UNIQUE(A DESC), UNIQUE(a COLLATE binary ASC))",
            "t",
        );

        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].name, "sqlite_autoindex_t_1");
        assert_eq!(specs[0].columns, vec!["A"]);
        assert_eq!(
            specs[0].key_sort_directions,
            vec![SortDirection::Desc],
            "equivalence ignores direction but retains the first declaration"
        );
        assert_eq!(
            specs[0].key_collations,
            vec![None],
            "None and explicit BINARY are equivalent but retain the first declaration"
        );
    }

    #[test]
    fn test_implicit_index_plan_uses_effective_column_collations() {
        let specs = plan_test_implicit_indexes(
            "CREATE TABLE t(
                a TEXT COLLATE RTRIM COLLATE NoCase,
                b TEXT COLLATE RTRIM,
                UNIQUE(a DESC, b),
                UNIQUE(A COLLATE nocase ASC, B COLLATE rtrim DESC),
                UNIQUE(a COLLATE BINARY, b)
            )",
            "t",
        );

        assert_eq!(specs.len(), 2);
        assert_eq!(
            specs[0].key_collations,
            vec![Some("NoCase".to_owned()), Some("RTRIM".to_owned())]
        );
        assert_eq!(
            specs[0].key_sort_directions,
            vec![SortDirection::Desc, SortDirection::Asc]
        );
        assert_eq!(
            specs[1].key_collations,
            vec![Some("BINARY".to_owned()), Some("RTRIM".to_owned())]
        );
    }

    #[test]
    fn test_implicit_index_plan_retains_first_constraint_direction_on_pk_promotion() {
        let rowid_specs =
            plan_test_implicit_indexes("CREATE TABLE t(a TEXT UNIQUE PRIMARY KEY DESC)", "t");
        assert_eq!(rowid_specs.len(), 1);
        assert!(rowid_specs[0].is_primary_key);
        assert_eq!(rowid_specs[0].key_sort_directions, vec![SortDirection::Asc]);

        let wr_specs = plan_test_implicit_indexes(
            "CREATE TABLE wr(a TEXT, UNIQUE(a DESC), PRIMARY KEY(a ASC)) WITHOUT ROWID",
            "wr",
        );
        assert_eq!(wr_specs.len(), 1);
        assert!(wr_specs[0].is_primary_key);
        assert_eq!(
            wr_specs[0].backing,
            ImplicitConstraintIndexBacking::WithoutRowidTableRoot
        );
        assert_eq!(wr_specs[0].key_sort_directions, vec![SortDirection::Desc]);
    }

    #[test]
    fn test_implicit_index_plan_collapses_column_unique_primary_key_to_wr_root() {
        let specs = plan_test_implicit_indexes(
            "CREATE TABLE wr(id TEXT UNIQUE PRIMARY KEY, value TEXT) WITHOUT ROWID",
            "wr",
        );

        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].name, "sqlite_autoindex_wr_1");
        assert_eq!(specs[0].columns, vec!["id"]);
        assert!(specs[0].is_primary_key);
        assert_eq!(
            specs[0].backing,
            ImplicitConstraintIndexBacking::WithoutRowidTableRoot
        );
        assert!(
            extract_unique_constraint_indexes_from_sql(
                "CREATE TABLE wr(id TEXT UNIQUE PRIMARY KEY, value TEXT) WITHOUT ROWID",
                "wr",
            )
            .unwrap()
            .is_empty()
        );
    }

    #[test]
    fn test_implicit_index_plan_collapses_both_table_constraint_orderings_to_wr_root() {
        for sql in [
            "CREATE TABLE wr(id TEXT, UNIQUE(id), PRIMARY KEY(id)) WITHOUT ROWID",
            "CREATE TABLE wr(id TEXT, PRIMARY KEY(id), UNIQUE(id)) WITHOUT ROWID",
        ] {
            let specs = plan_test_implicit_indexes(sql, "wr");
            assert_eq!(specs.len(), 1, "{sql}: {specs:?}");
            assert_eq!(specs[0].name, "sqlite_autoindex_wr_1", "{sql}");
            assert_eq!(specs[0].columns, vec!["id"], "{sql}");
            assert!(specs[0].is_primary_key, "{sql}");
            assert_eq!(
                specs[0].backing,
                ImplicitConstraintIndexBacking::WithoutRowidTableRoot,
                "{sql}"
            );
        }
    }

    #[test]
    fn test_implicit_index_plan_merges_compatible_conflict_actions() {
        let promoted = plan_test_implicit_indexes(
            "CREATE TABLE t(
                a TEXT,
                UNIQUE(a),
                UNIQUE(A COLLATE binary) ON CONFLICT IGNORE
            )",
            "t",
        );
        assert_eq!(promoted.len(), 1);
        assert_eq!(promoted[0].conflict_action, Some(ConflictAction::Ignore));

        let same = plan_test_implicit_indexes(
            "CREATE TABLE t(
                a TEXT,
                UNIQUE(a) ON CONFLICT REPLACE,
                PRIMARY KEY(A) ON CONFLICT REPLACE
            )",
            "t",
        );
        assert_eq!(same.len(), 1);
        assert!(same[0].is_primary_key);
        assert_eq!(same[0].conflict_action, Some(ConflictAction::Replace));
    }

    #[test]
    fn test_implicit_index_plan_rejects_conflicting_explicit_conflict_actions() {
        let Some(Statement::CreateTable(create)) = parse_single_statement(
            "CREATE TABLE t(
                a TEXT,
                UNIQUE(a) ON CONFLICT IGNORE,
                UNIQUE(A COLLATE binary) ON CONFLICT REPLACE
            )",
        ) else {
            panic!("test CREATE TABLE must parse");
        };
        let error = plan_implicit_constraint_indexes(&create, "t").unwrap_err();
        assert!(matches!(
            error,
            FrankenError::FunctionError(ref detail)
                if detail == "conflicting ON CONFLICT clauses specified"
        ));
    }

    #[test]
    fn test_implicit_index_plan_rejects_expression_and_unknown_constraint_terms() {
        let Some(Statement::CreateTable(expression_create)) =
            parse_single_statement("CREATE TABLE t(a TEXT, UNIQUE(a || 'x'))")
        else {
            panic!("expression constraint test CREATE TABLE must parse");
        };
        let expression_error =
            plan_implicit_constraint_indexes(&expression_create, "t").unwrap_err();
        assert!(matches!(
            expression_error,
            FrankenError::FunctionError(ref detail)
                if detail == "expressions prohibited in PRIMARY KEY and UNIQUE constraints"
        ));

        let Some(Statement::CreateTable(unknown_create)) =
            parse_single_statement("CREATE TABLE t(a TEXT, UNIQUE(missing))")
        else {
            panic!("unknown-column constraint test CREATE TABLE must parse");
        };
        let unknown_error = plan_implicit_constraint_indexes(&unknown_create, "t").unwrap_err();
        assert!(matches!(
            unknown_error,
            FrankenError::FunctionError(ref detail) if detail == "no such column: missing"
        ));
    }

    #[test]
    fn test_auxiliary_implicit_index_inventory_is_an_exact_set() {
        let specs = plan_test_implicit_indexes(
            "CREATE TABLE wr(
                id TEXT PRIMARY KEY,
                tenant TEXT UNIQUE,
                UNIQUE(id, tenant)
            ) WITHOUT ROWID",
            "wr",
        );

        validate_auxiliary_implicit_index_inventory(
            "wr",
            &specs,
            &[
                "SQLITE_AUTOINDEX_WR_2".to_owned(),
                "sqlite_autoindex_wr_3".to_owned(),
            ],
        )
        .unwrap();

        for (actual, expected_fragment) in [
            (
                vec!["sqlite_autoindex_wr_2".to_owned()],
                "sqlite_autoindex_wr_3",
            ),
            (
                vec![
                    "sqlite_autoindex_wr_1".to_owned(),
                    "sqlite_autoindex_wr_2".to_owned(),
                    "sqlite_autoindex_wr_3".to_owned(),
                ],
                "sqlite_autoindex_wr_1",
            ),
            (
                vec![
                    "sqlite_autoindex_wr_2".to_owned(),
                    "sqlite_autoindex_wr_2".to_owned(),
                    "sqlite_autoindex_wr_3".to_owned(),
                ],
                "duplicate=[\"sqlite_autoindex_wr_2\"]",
            ),
        ] {
            let error =
                validate_auxiliary_implicit_index_inventory("wr", &specs, &actual).unwrap_err();
            assert!(
                matches!(
                    &error,
                    FrankenError::DatabaseCorrupt { detail }
                        if detail.contains(expected_fragment)
                ),
                "{error}"
            );
        }
    }

    #[test]
    fn test_is_strict_table_sql_detects_strict_options() {
        assert!(is_strict_table_sql(
            "CREATE TABLE s (id INTEGER, body TEXT) STRICT"
        ));
        assert!(is_strict_table_sql(
            "CREATE TABLE s (id INTEGER) WITHOUT ROWID, STRICT;"
        ));
        assert!(!is_strict_table_sql(
            "CREATE TABLE s (id INTEGER, body TEXT) WITHOUT ROWID"
        ));
    }

    #[test]
    fn test_is_without_rowid_table_sql_detects_option() {
        assert!(is_without_rowid_table_sql(
            "CREATE TABLE s (id INTEGER PRIMARY KEY, body TEXT) WITHOUT ROWID"
        ));
        assert!(is_without_rowid_table_sql(
            "CREATE TABLE s (id INTEGER PRIMARY KEY, body TEXT) WITHOUT ROWID, STRICT;"
        ));
        assert!(!is_without_rowid_table_sql(
            "CREATE TABLE s (id INTEGER PRIMARY KEY, body TEXT) STRICT"
        ));
    }

    #[test]
    fn test_is_autoincrement_table_sql_detects_keyword() {
        assert!(is_autoincrement_table_sql(
            "CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)"
        ));
        assert!(!is_autoincrement_table_sql(
            "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)"
        ));
    }

    #[test]
    fn test_is_autoincrement_table_sql_ignores_default_literal_keyword() {
        assert!(!is_autoincrement_table_sql(
            "CREATE TABLE t(id INTEGER PRIMARY KEY, note TEXT DEFAULT 'AUTOINCREMENT')"
        ));
        assert!(!is_autoincrement_table_sql(
            "CREATE TABLE t(id INTEGER PRIMARY KEY, note TEXT DEFAULT 'AUTOINCREMENT') trailing"
        ));
    }

    #[test]
    fn test_parse_columns_from_create_sql_populates_strict_types() {
        let sql = "CREATE TABLE strict_cols (id INTEGER, score REAL, body TEXT, payload BLOB, any_col ANY) STRICT";
        let cols = parse_columns_from_create_sql(sql);
        assert_eq!(cols.len(), 5);
        assert_eq!(cols[0].strict_type, Some(StrictColumnType::Integer));
        assert_eq!(cols[1].strict_type, Some(StrictColumnType::Real));
        assert_eq!(cols[2].strict_type, Some(StrictColumnType::Text));
        assert_eq!(cols[3].strict_type, Some(StrictColumnType::Blob));
        assert_eq!(cols[4].strict_type, Some(StrictColumnType::Any));
    }

    #[test]
    fn test_parse_columns_from_sqlite_master_sql_ignores_virtual_table_options() {
        let sql =
            "CREATE VIRTUAL TABLE docs USING fts5(subject, body, tokenize='porter', prefix='2 3')";
        let cols = parse_columns_from_sqlite_master_sql(sql);
        let names: Vec<&str> = cols.iter().map(|column| column.name.as_str()).collect();
        assert_eq!(names, vec!["subject", "body"]);
    }

    #[test]
    fn test_extract_check_constraints_from_sql_ignores_literal_check_text() {
        let sql = "CREATE TABLE t (note TEXT DEFAULT 'CHECK(fake)', CHECK(length(note) > 0))";
        let checks = extract_check_constraints_from_sql(sql);
        assert_eq!(checks, vec!["length(note) > 0".to_owned()]);
    }

    #[test]
    fn test_check_constraint_ast_preserves_column_ownership_with_conflict_clause() {
        // SQLite accepts a conflict clause after a table CHECK for historical
        // compatibility. The AST must consume and preserve that suffix without
        // flattening a neighboring column CHECK into table scope.
        let sql = r#"CREATE TABLE t(
            "owned col" TEXT DEFAULT 'CHECK(fake)' CHECK(length("owned col") > 0),
            b INTEGER,
            CONSTRAINT/*name*/ table_check CHECK/*expr*/(b > 0) ON CONFLICT FAIL
        )"#;
        let checks = extract_check_constraints_with_owners_from_sql(sql);
        assert_eq!(
            checks,
            vec![
                CheckConstraint {
                    expr: r#"length("owned col") > 0"#.to_owned(),
                    owner_column: Some("owned col".to_owned()),
                },
                CheckConstraint {
                    expr: "b > 0".to_owned(),
                    owner_column: None,
                },
            ]
        );
    }

    #[test]
    fn test_foreign_key_ast_preserves_ownership_and_actions_with_check_conflict() {
        // The trailing CHECK conflict clause must not disturb ownership or
        // action metadata reconstructed from the same parsed table definition.
        let sql = r#"CREATE TABLE child(
            "owned col" INTEGER REFERENCES parent(id)
                ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,
            keep INTEGER,
            CONSTRAINT fk_keep FOREIGN KEY(keep) REFERENCES "parent table"("id col")
                ON UPDATE RESTRICT NOT DEFERRABLE INITIALLY DEFERRED,
            CHECK(keep > 0) ON CONFLICT FAIL
        )"#;
        let columns = parse_columns_from_create_sql(sql);
        let foreign_keys = extract_foreign_keys_from_sql(sql, &columns);

        assert_eq!(foreign_keys.len(), 2);
        assert_eq!(foreign_keys[0].child_columns, vec![0]);
        assert_eq!(foreign_keys[0].owner_column.as_deref(), Some("owned col"));
        assert_eq!(foreign_keys[0].parent_table, "parent");
        assert_eq!(foreign_keys[0].parent_columns, vec!["id"]);
        assert_eq!(foreign_keys[0].on_delete, FkActionType::Cascade);
        assert!(foreign_keys[0].deferred);

        assert_eq!(foreign_keys[1].child_columns, vec![1]);
        assert_eq!(foreign_keys[1].owner_column, None);
        assert_eq!(foreign_keys[1].on_update, FkActionType::Restrict);
        assert!(!foreign_keys[1].deferred);
    }

    #[test]
    fn test_type_to_affinity_mapping() {
        assert_eq!(type_to_affinity("INTEGER"), 'D');
        assert_eq!(type_to_affinity("INT"), 'D');
        assert_eq!(type_to_affinity("REAL"), 'E');
        assert_eq!(type_to_affinity("FLOAT"), 'E');
        assert_eq!(type_to_affinity("TEXT"), 'B');
        assert_eq!(type_to_affinity("VARCHAR"), 'B');
        assert_eq!(type_to_affinity("BLOB"), 'A');
        assert_eq!(type_to_affinity("NUMERIC"), 'C');
    }

    #[test]
    fn test_parse_create_index_sql_preserves_quoted_collations_and_comments() {
        let sql = r#"CREATE INDEX "idx(words)" ON "items(table)" (
            "last, name" COLLATE /* keep comment invisible */ [RTRIM] DESC,
            code/* comma, paren ), and COLLATE text stay in comment */COLLATE 'BINARY',
            tag COLLATE DESC,
            ord COLLATE [DESC] DESC
        ) /* index tail */ WHERE active = 1"#;

        let idx = parse_create_index_sql_to_schema("idx(words)", 7, sql).unwrap();

        assert_eq!(
            idx.columns,
            vec![
                "last, name".to_owned(),
                "code".to_owned(),
                "tag".to_owned(),
                "ord".to_owned()
            ]
        );
        assert_eq!(
            idx.key_collations,
            vec![
                Some("RTRIM".to_owned()),
                Some("BINARY".to_owned()),
                Some("DESC".to_owned()),
                Some("DESC".to_owned())
            ]
        );
        assert_eq!(
            idx.key_sort_directions,
            vec![
                SortDirection::Desc,
                SortDirection::Asc,
                SortDirection::Asc,
                SortDirection::Desc
            ]
        );
        assert_eq!(idx.where_clause.as_deref(), Some("active = 1"));
    }

    #[test]
    fn test_parse_create_index_sql_preserves_expression_terms() {
        let sql =
            "CREATE UNIQUE INDEX uq_agents_name_ci ON agents(lower(name) DESC) WHERE is_active = 1";

        let idx = parse_create_index_sql_to_schema("uq_agents_name_ci", 7, sql).unwrap();

        assert!(idx.columns.is_empty());
        assert_eq!(idx.key_expressions.len(), 1);
        assert_eq!(idx.key_expressions[0].to_ascii_lowercase(), "lower(name)");
        assert_eq!(idx.key_sort_directions, vec![SortDirection::Desc]);
        assert_eq!(idx.where_clause.as_deref(), Some("is_active = 1"));
        assert!(idx.is_unique);
    }

    #[test]
    fn test_build_create_index_sql_preserves_unique_collation_and_direction() {
        let terms = [
            CreateIndexSqlTerm {
                column_name: "project_id",
                collation: None,
                direction: Some(SortDirection::Asc),
            },
            CreateIndexSqlTerm {
                column_name: "name",
                collation: Some("NOCASE"),
                direction: Some(SortDirection::Desc),
            },
        ];

        let sql = build_create_index_sql(
            "idx_agents_project_name_nocase",
            "agents",
            true,
            &terms,
            None,
        );

        assert_eq!(
            sql,
            "CREATE UNIQUE INDEX \"idx_agents_project_name_nocase\" ON \"agents\" (\"project_id\" ASC, \"name\" COLLATE \"NOCASE\" DESC)"
        );
    }

    #[test]
    fn test_build_create_index_sql_escapes_embedded_quotes_in_identifiers() {
        let terms = [CreateIndexSqlTerm {
            column_name: "na\"me",
            collation: Some("NO\"CASE"),
            direction: Some(SortDirection::Desc),
        }];

        let sql = build_create_index_sql("idx\"q", "ta\"ble", true, &terms, None);

        assert_eq!(
            sql,
            "CREATE UNIQUE INDEX \"idx\"\"q\" ON \"ta\"\"ble\" (\"na\"\"me\" COLLATE \"NO\"\"CASE\" DESC)"
        );
    }

    #[test]
    fn test_index_sql_preserves_explicit_reserved_prefix_definition() {
        let mut original_ddl = HashMap::new();
        original_ddl.insert(
            "sqlite_autoindex_link_table_v23_1".to_owned(),
            "CREATE UNIQUE INDEX \"sqlite_autoindex_link_table_v23_1\" ON \"link_table\"(a,b)"
                .to_owned(),
        );

        let sql = index_sql_for_persistence(
            "sqlite_autoindex_link_table_v23_1",
            "link_table",
            &original_ddl,
            || panic!("preserved explicit DDL must win over prefix classification"),
        );

        assert_eq!(
            sql.as_deref(),
            Some(
                "CREATE UNIQUE INDEX \"sqlite_autoindex_link_table_v23_1\" ON \"link_table\"(a,b)"
            )
        );
    }

    #[test]
    fn test_index_sql_keeps_true_implicit_autoindex_null() {
        let original_ddl = HashMap::<String, String>::new();

        let sql = index_sql_for_persistence(
            "sqlite_autoindex_link_table_1",
            "link_table",
            &original_ddl,
            || panic!("implicit autoindex SQL must not be synthesized"),
        );

        assert_eq!(sql, None);
    }

    #[test]
    fn test_autoindex_ordinal_requires_canonical_positive_ascii_decimal() {
        assert_eq!(
            parse_autoindex_ordinal("sqlite_autoindex_link_table_1", "link_table"),
            Some(1)
        );
        for noncanonical in [
            "sqlite_autoindex_link_table_0",
            "sqlite_autoindex_link_table_01",
            "sqlite_autoindex_link_table_+1",
            "sqlite_autoindex_link_table_١",
            "sqlite_autoindex_other_table_1",
        ] {
            assert_eq!(
                parse_autoindex_ordinal(noncanonical, "link_table"),
                None,
                "{noncanonical} must not classify as an implicit autoindex"
            );
        }
        let overflowing = format!("sqlite_autoindex_link_table_{}0", usize::MAX);
        assert_eq!(parse_autoindex_ordinal(&overflowing, "link_table"), None);
    }

    #[test]
    fn test_index_sql_synthesizes_unrecorded_explicit_index() {
        let original_ddl = HashMap::<String, String>::new();

        let sql =
            index_sql_for_persistence("idx_link_table_a", "link_table", &original_ddl, || {
                "CREATE INDEX idx_link_table_a ON link_table(a)".to_owned()
            });

        assert_eq!(
            sql.as_deref(),
            Some("CREATE INDEX idx_link_table_a ON link_table(a)")
        );
    }

    #[test]
    fn test_index_sql_synthesizes_noncanonical_reserved_prefix_without_ddl() {
        let original_ddl = HashMap::<String, String>::new();

        let sql = index_sql_for_persistence(
            "sqlite_autoindex_link_table_v23_1",
            "link_table",
            &original_ddl,
            || {
                "CREATE UNIQUE INDEX \"sqlite_autoindex_link_table_v23_1\" ON \"link_table\"(a,b)"
                    .to_owned()
            },
        );

        assert_eq!(
            sql.as_deref(),
            Some(
                "CREATE UNIQUE INDEX \"sqlite_autoindex_link_table_v23_1\" ON \"link_table\"(a,b)"
            )
        );
    }

    #[test]
    fn test_reserved_prefix_explicit_index_survives_without_original_table_ddl() {
        asupersync::test_utils::run_test(|| async {
            const TABLE_SQL: &str = "CREATE TABLE link_table(\
            a INTEGER NOT NULL,\
            b INTEGER NOT NULL,\
            PRIMARY KEY(a,b)\
        )";
            const INDEX_NAME: &str = "sqlite_autoindex_link_table_v23_1";
            const INDEX_SQL: &str = "CREATE UNIQUE INDEX \
            \"sqlite_autoindex_link_table_v23_1\" ON \"link_table\"(a,b)";

            let dir = tempfile::tempdir().unwrap();
            let source_path = dir.path().join("reserved-prefix-source.db");
            let rebuilt_path = dir.path().join("reserved-prefix-rebuilt.db");

            {
                let sqlite = rusqlite::Connection::open(&source_path).unwrap();
                sqlite
                    .execute_batch(&format!(
                        r"
                    {TABLE_SQL};
                    CREATE UNIQUE INDEX legacy_unique ON link_table(a,b);
                    INSERT INTO link_table VALUES (1,2), (3,4);
                    PRAGMA writable_schema=ON;
                    UPDATE sqlite_master
                       SET name='{INDEX_NAME}', sql='{INDEX_SQL}'
                     WHERE type='index' AND name='legacy_unique';
                    PRAGMA schema_version=2;
                    "
                    ))
                    .unwrap();
            }

            {
                let sqlite = rusqlite::Connection::open(&source_path).unwrap();
                let quick_check: String = sqlite
                    .query_row("PRAGMA quick_check;", [], |row| row.get(0))
                    .unwrap();
                let integrity_check: String = sqlite
                    .query_row("PRAGMA integrity_check;", [], |row| row.get(0))
                    .unwrap();
                assert_eq!(quick_check, "ok");
                assert_eq!(integrity_check, "ok");
            }

            let loaded = load_test_db(&source_path).await.unwrap();
            let table = loaded
                .schema
                .iter()
                .find(|table| table.name == "link_table")
                .unwrap();
            assert!(
                table.indexes.iter().any(|index| index.name == INDEX_NAME),
                "a non-NULL CREATE INDEX entry is explicit even with a reserved-prefix name"
            );

            let mut original_ddl = HashMap::new();
            // Deliberately omit the table DDL so persistence must reconstruct it
            // from TableSchema. The explicit reserved-prefix index DDL remains the
            // provenance signal that prevents a phantom UNIQUE table constraint.
            original_ddl.insert(INDEX_NAME.to_owned(), INDEX_SQL.to_owned());
            let header = DatabaseHeader {
                page_size: DEFAULT_PAGE_SIZE,
                schema_cookie: loaded.schema_cookie,
                change_counter: loaded.change_counter,
                version_valid_for: loaded.change_counter,
                ..DatabaseHeader::default()
            };
            persist_to_sqlite_with_header_and_master_entries(
                &Cx::new(),
                &rebuilt_path,
                &loaded.schema,
                &loaded.db,
                &header,
                &[],
                &original_ddl,
            )
            .await
            .unwrap();

            let sqlite = rusqlite::Connection::open(&rebuilt_path).unwrap();
            let stored_table_sql: String = sqlite
                .query_row(
                    "SELECT sql FROM sqlite_master WHERE type='table' AND name='link_table';",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            let stored_sql: String = sqlite
                .query_row(
                    "SELECT sql FROM sqlite_master WHERE type='index' AND name=?1;",
                    [INDEX_NAME],
                    |row| row.get(0),
                )
                .unwrap();
            let row_count: i64 = sqlite
                .query_row("SELECT COUNT(*) FROM link_table;", [], |row| row.get(0))
                .unwrap();
            let quick_check: String = sqlite
                .query_row("PRAGMA quick_check;", [], |row| row.get(0))
                .unwrap();
            let integrity_check: String = sqlite
                .query_row("PRAGMA integrity_check;", [], |row| row.get(0))
                .unwrap();
            assert!(
                !stored_table_sql.to_ascii_uppercase().contains("UNIQUE"),
                "reconstructed table DDL must not duplicate the explicit reserved-prefix index: {stored_table_sql}"
            );
            assert_eq!(stored_sql, INDEX_SQL);
            assert_eq!(row_count, 2);
            assert_eq!(quick_check, "ok");
            assert_eq!(integrity_check, "ok");
            drop(sqlite);

            let reopened = load_test_db(&rebuilt_path).await.unwrap();
            let table = reopened
                .schema
                .iter()
                .find(|table| table.name == "link_table")
                .unwrap();
            assert!(table.indexes.iter().any(|index| index.name == INDEX_NAME));
        });
    }

    #[test]
    fn test_build_create_expression_index_sql_does_not_duplicate_collation() {
        let expressions = vec!["lower(name) COLLATE NOCASE".to_owned()];
        let collations = vec![Some("NOCASE".to_owned())];
        let directions = vec![SortDirection::Desc];

        let sql = build_create_expression_index_sql(
            "idx_expr",
            "agents",
            false,
            &expressions,
            &collations,
            &directions,
            Some("is_active = 1"),
        );

        assert_eq!(
            sql,
            "CREATE INDEX \"idx_expr\" ON \"agents\" (lower(name) COLLATE NOCASE DESC) WHERE is_active = 1"
        );
    }

    #[test]
    fn test_persist_to_sqlite_keeps_expression_index_btree_and_schema() {
        asupersync::test_utils::run_test(|| async {
            let dir = tempfile::tempdir().unwrap();
            let db_path = dir.path().join("expression-index-persist.db");
            let cx = Cx::new();

            let mut db = MemDatabase::new();
            db.create_table_at(2, 3);
            let table_data = db.get_table_mut(2).unwrap();
            table_data.insert_row(
                1,
                vec![
                    SqliteValue::Integer(1),
                    SqliteValue::Text("Alpha".into()),
                    SqliteValue::Integer(1),
                ],
            );
            table_data.insert_row(
                2,
                vec![
                    SqliteValue::Integer(2),
                    SqliteValue::Text("Dormant".into()),
                    SqliteValue::Integer(0),
                ],
            );

            let schema = vec![TableSchema {
                name: "agents".to_owned(),
                root_page: 2,
                columns: vec![
                    ColumnInfo {
                        name: "id".to_owned(),
                        affinity: 'D',
                        is_ipk: true,
                        type_name: Some("INTEGER".to_owned()),
                        notnull: false,
                        unique: false,
                        default_value: None,
                        strict_type: None,
                        generated_expr: None,
                        generated_stored: None,
                        collation: None,
                        conflict_action: None,
                    },
                    ColumnInfo {
                        name: "name".to_owned(),
                        affinity: 'B',
                        is_ipk: false,
                        type_name: Some("TEXT".to_owned()),
                        notnull: true,
                        unique: false,
                        default_value: None,
                        strict_type: None,
                        generated_expr: None,
                        generated_stored: None,
                        collation: None,
                        conflict_action: None,
                    },
                    ColumnInfo {
                        name: "is_active".to_owned(),
                        affinity: 'D',
                        is_ipk: false,
                        type_name: Some("INTEGER".to_owned()),
                        notnull: true,
                        unique: false,
                        default_value: Some("1".to_owned()),
                        strict_type: None,
                        generated_expr: None,
                        generated_stored: None,
                        collation: None,
                        conflict_action: None,
                    },
                ],
                indexes: vec![IndexSchema {
                    name: "uq_agents_name_ci".to_owned(),
                    root_page: 3,
                    columns: Vec::new(),
                    key_expressions: vec!["lower(name)".to_owned()],
                    key_sort_directions: vec![SortDirection::Asc],
                    where_clause: Some("is_active = 1".to_owned()),
                    is_unique: true,
                    key_collations: vec![None],
                    conflict_action: None,
                }],
                strict: false,
                without_rowid: false,
                primary_key_constraints: vec![PrimaryKeyConstraint::new(
                    vec!["id".to_owned()],
                    vec![SortDirection::Asc],
                    vec![None],
                )],
                foreign_keys: Vec::new(),
                check_constraints: Vec::new(),
            }];
            let header = DatabaseHeader {
                page_size: DEFAULT_PAGE_SIZE,
                schema_cookie: 1,
                change_counter: 1,
                version_valid_for: 1,
                ..DatabaseHeader::default()
            };
            let mut original_ddl = HashMap::new();
            original_ddl.insert(
            "agents".to_owned(),
            "CREATE TABLE agents (id INTEGER PRIMARY KEY, name TEXT NOT NULL, is_active INTEGER NOT NULL DEFAULT 1)"
                .to_owned(),
        );
            original_ddl.insert(
                "uq_agents_name_ci".to_owned(),
                "CREATE UNIQUE INDEX uq_agents_name_ci ON agents(lower(name)) WHERE is_active = 1"
                    .to_owned(),
            );

            persist_to_sqlite_with_header_and_master_entries(
                &cx,
                &db_path,
                &schema,
                &db,
                &header,
                &[],
                &original_ddl,
            )
            .await
            .unwrap();

            let conn = rusqlite::Connection::open(&db_path).unwrap();
            let integrity: String = conn
                .query_row("PRAGMA integrity_check;", [], |row| row.get(0))
                .unwrap();
            assert_eq!(integrity, "ok");
            let index_sql: String = conn
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type='index' AND name='uq_agents_name_ci';",
                [],
                |row| row.get(0),
            )
            .unwrap();
            assert!(
                index_sql.to_ascii_lowercase().contains("lower(name)")
                    && index_sql
                        .to_ascii_lowercase()
                        .contains("where is_active = 1"),
                "expression index SQL should be preserved: {index_sql}"
            );
            let duplicate = conn.execute(
                "INSERT INTO agents(name, is_active) VALUES ('ALPHA', 1);",
                [],
            );
            assert!(
                duplicate.is_err(),
                "persisted expression index should still enforce active-name uniqueness"
            );
        });
    }

    #[test]
    fn test_overwrite_existing_file() {
        asupersync::test_utils::run_test(|| async {
            let dir = tempfile::tempdir().unwrap();
            let db_path = dir.path().join("overwrite.db");

            // Write once.
            let (schema, db) = make_test_schema_and_db();
            persist_test_db(&db_path, &schema, &db, 0, 0).await.unwrap();

            // Overwrite with empty.
            persist_test_db(&db_path, &[], &MemDatabase::new(), 0, 0)
                .await
                .unwrap();

            let loaded = load_test_db(&db_path).await.unwrap();
            assert!(loaded.schema.is_empty());
        });
    }

    #[test]
    fn test_load_from_sqlite_keeps_materialized_virtual_tables_with_real_root_page() {
        asupersync::test_utils::run_test(|| async {
            let dir = tempfile::tempdir().unwrap();
            let db_path = dir.path().join("materialized_vtab_load.db");
            let db_str = db_path.to_string_lossy().to_string();

            {
                let conn = crate::connection::Connection::open(&db_str).await.unwrap();
                conn.execute(
                    "CREATE VIRTUAL TABLE docs USING fts5(subject, body, tokenize='porter')",
                )
                .await
                .unwrap();
                conn.execute(
                    "INSERT INTO docs(rowid, subject, body) VALUES (1, 'Hello', 'Rust world')",
                )
                .await
                .unwrap();
                conn.execute(
                    "INSERT INTO docs(rowid, subject, body) VALUES (2, 'Other', 'Nothing')",
                )
                .await
                .unwrap();
                conn.close().await.unwrap();
            }

            let loaded = load_test_db(&db_path).await.unwrap();
            // FrankenSQLite-created FTS5 tables are now stock-compatible
            // rootpage=0 virtual tables. The low-level compat loader deliberately
            // skips rootpage=0 virtual-table catalog rows (they have no
            // materialized root b-tree of their own; the live vtab is reconstructed
            // at the higher connection-reload layer instead), so the `docs` row is
            // NOT present here — but the durable document content it persisted DOES
            // survive, in the positive-rootpage `docs_content` shadow table.
            assert!(
                loaded
                    .schema
                    .iter()
                    .all(|table| !table.name.eq_ignore_ascii_case("docs")),
                "rootpage=0 FTS5 virtual-table catalog row must be skipped by the low-level loader"
            );

            // The persisted document content lives in the `docs_content` shadow
            // table, laid out as (id INTEGER PRIMARY KEY, c0=subject, c1=body).
            let content = loaded
                .schema
                .iter()
                .find(|table| table.name.eq_ignore_ascii_case("docs_content"))
                .expect("FTS5 content shadow table should survive direct load");
            let content_columns: Vec<&str> = content
                .columns
                .iter()
                .map(|column| column.name.as_str())
                .collect();
            assert_eq!(content_columns, vec!["id", "c0", "c1"]);
            assert!(
                content.root_page > 0,
                "the _content shadow table is a real positive-rootpage b-tree"
            );
            let mem_table = loaded
                .db
                .get_table(content.root_page)
                .expect("loaded content shadow table should exist in MemDatabase");
            // The _content shadow b-tree stores the full record (id, c0, c1): the
            // INTEGER PRIMARY KEY `id` is both the rowid and the first record value,
            // followed by the document columns subject (c0) and body (c1).
            let rows: Vec<_> = mem_table.iter_rows().collect();
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].0, 1);
            assert_eq!(rows[0].1[0], SqliteValue::Integer(1));
            assert_eq!(rows[0].1[1], SqliteValue::Text("Hello".into()));
            assert_eq!(rows[0].1[2], SqliteValue::Text("Rust world".into()));
            assert_eq!(rows[1].0, 2);
            assert_eq!(rows[1].1[0], SqliteValue::Integer(2));
            assert_eq!(rows[1].1[1], SqliteValue::Text("Other".into()));
            assert_eq!(rows[1].1[2], SqliteValue::Text("Nothing".into()));
        });
    }

    #[test]
    fn test_load_from_sqlite_rejects_non_virtual_table_with_rootpage_zero() {
        asupersync::test_utils::run_test(|| async {
            let dir = tempfile::tempdir().unwrap();
            let db_path = dir.path().join("compat_corrupt_rootpage_zero.db");

            {
                let conn = rusqlite::Connection::open(&db_path).unwrap();
                conn.execute_batch(
                    r"
                CREATE TABLE docs (id INTEGER PRIMARY KEY, title TEXT);
                INSERT INTO docs VALUES (1, 'hello');
                PRAGMA writable_schema = ON;
                UPDATE sqlite_master SET rootpage = 0 WHERE name = 'docs';
                PRAGMA writable_schema = OFF;
                ",
                )
                .unwrap();
            }

            let err = match load_test_db(&db_path).await {
                Ok(_) => panic!("corrupt rootpage should fail load"),
                Err(err) => err,
            };
            let message = err.to_string();
            assert!(
                message.contains("rootpage 0") || message.contains("root page"),
                "unexpected load error: {message}"
            );
        });
    }

    #[test]
    fn test_load_from_sqlite_rejects_negative_rootpage() {
        asupersync::test_utils::run_test(|| async {
            let dir = tempfile::tempdir().unwrap();
            let db_path = dir.path().join("compat_corrupt_rootpage_negative.db");

            {
                let conn = rusqlite::Connection::open(&db_path).unwrap();
                conn.execute_batch(
                    r"
                CREATE TABLE docs (id INTEGER PRIMARY KEY, title TEXT);
                INSERT INTO docs VALUES (1, 'hello');
                PRAGMA writable_schema = ON;
                UPDATE sqlite_master SET rootpage = -7 WHERE name = 'docs';
                PRAGMA writable_schema = OFF;
                ",
                )
                .unwrap();
            }

            let err = match load_test_db(&db_path).await {
                Ok(_) => panic!("negative rootpage should fail load"),
                Err(err) => err,
            };
            let message = err.to_string();
            assert!(
                message.contains("rootpage -7") || message.contains("invalid rootpage"),
                "unexpected load error: {message}"
            );
        });
    }

    #[test]
    fn test_load_from_sqlite_rejects_rootpage_above_supported_range() {
        asupersync::test_utils::run_test(|| async {
            let dir = tempfile::tempdir().unwrap();
            let db_path = dir.path().join("compat_corrupt_rootpage_large.db");

            {
                let conn = rusqlite::Connection::open(&db_path).unwrap();
                conn.execute_batch(
                    r"
                CREATE TABLE docs (id INTEGER PRIMARY KEY, title TEXT);
                INSERT INTO docs VALUES (1, 'hello');
                PRAGMA writable_schema = ON;
                UPDATE sqlite_master SET rootpage = 2147483648 WHERE name = 'docs';
                PRAGMA writable_schema = OFF;
                ",
                )
                .unwrap();
            }

            let err = match load_test_db(&db_path).await {
                Ok(_) => panic!("oversized rootpage should fail load"),
                Err(err) => err,
            };
            let message = err.to_string();
            assert!(
                message.contains("supported range")
                    || message.contains("out-of-range")
                    || message.contains("2147483648"),
                "unexpected load error: {message}"
            );
        });
    }

    #[test]
    fn test_load_from_sqlite_rejects_index_rootpage_above_supported_range() {
        asupersync::test_utils::run_test(|| async {
            let dir = tempfile::tempdir().unwrap();
            let db_path = dir.path().join("compat_corrupt_index_rootpage_large.db");

            {
                let conn = rusqlite::Connection::open(&db_path).unwrap();
                conn.execute_batch(
                    r"
                CREATE TABLE docs (id INTEGER PRIMARY KEY, title TEXT);
                CREATE INDEX docs_title_idx ON docs(title);
                PRAGMA writable_schema = ON;
                UPDATE sqlite_master SET rootpage = 2147483648 WHERE name = 'docs_title_idx';
                PRAGMA writable_schema = OFF;
                ",
                )
                .unwrap();
            }

            let err = match load_test_db(&db_path).await {
                Ok(_) => panic!("oversized index rootpage should fail load"),
                Err(err) => err,
            };
            let message = err.to_string();
            assert!(
                message.contains("docs_title_idx") && message.contains("2147483648"),
                "unexpected load error: {message}"
            );
        });
    }

    #[test]
    fn test_load_from_sqlite_rejects_non_integer_table_rootpage() {
        asupersync::test_utils::run_test(|| async {
            let dir = tempfile::tempdir().unwrap();
            let db_path = dir.path().join("compat_non_integer_table_rootpage.db");

            {
                let conn = rusqlite::Connection::open(&db_path).unwrap();
                conn.execute_batch(
                    "CREATE TABLE docs (id INTEGER PRIMARY KEY, title TEXT);
                     PRAGMA writable_schema = ON;
                     UPDATE sqlite_master
                     SET rootpage = 'not-an-integer'
                     WHERE type = 'table' AND name = 'docs';
                     PRAGMA writable_schema = OFF;",
                )
                .unwrap();
                let root_storage_class: String = conn
                    .query_row(
                        "SELECT typeof(rootpage) FROM sqlite_master
                         WHERE type = 'table' AND name = 'docs'",
                        [],
                        |row| row.get(0),
                    )
                    .unwrap();
                assert_eq!(root_storage_class, "text");
            }

            assert_compat_rejects_catalog_corruption(
                &db_path,
                &["table", "docs", "non-integer rootpage"],
            )
            .await;
        });
    }

    #[test]
    fn test_load_from_sqlite_rejects_non_integer_explicit_index_rootpage() {
        asupersync::test_utils::run_test(|| async {
            let dir = tempfile::tempdir().unwrap();
            let db_path = dir.path().join("compat_non_integer_index_rootpage.db");

            {
                let conn = rusqlite::Connection::open(&db_path).unwrap();
                conn.execute_batch(
                    "CREATE TABLE docs (id INTEGER PRIMARY KEY, title TEXT);
                     CREATE INDEX docs_title_idx ON docs(title);
                     PRAGMA writable_schema = ON;
                     UPDATE sqlite_master
                     SET rootpage = 'not-an-integer'
                     WHERE type = 'index' AND name = 'docs_title_idx';
                     PRAGMA writable_schema = OFF;",
                )
                .unwrap();
                let root_storage_class: String = conn
                    .query_row(
                        "SELECT typeof(rootpage) FROM sqlite_master
                         WHERE type = 'index' AND name = 'docs_title_idx'",
                        [],
                        |row| row.get(0),
                    )
                    .unwrap();
                assert_eq!(root_storage_class, "text");
            }

            assert_compat_rejects_catalog_corruption(
                &db_path,
                &["index", "docs_title_idx", "non-integer rootpage"],
            )
            .await;
        });
    }

    #[test]
    fn test_load_from_sqlite_rejects_explicit_index_with_missing_parent_table() {
        asupersync::test_utils::run_test(|| async {
            let dir = tempfile::tempdir().unwrap();
            let db_path = dir.path().join("compat_explicit_index_missing_parent.db");

            {
                let conn = rusqlite::Connection::open(&db_path).unwrap();
                conn.execute_batch(
                    "CREATE TABLE docs (id INTEGER PRIMARY KEY, title TEXT);
                     CREATE INDEX docs_title_idx ON docs(title);
                     PRAGMA writable_schema = ON;
                     UPDATE sqlite_master
                     SET tbl_name = 'missing_docs'
                     WHERE type = 'index' AND name = 'docs_title_idx';
                     PRAGMA writable_schema = OFF;",
                )
                .unwrap();
            }

            assert_compat_rejects_catalog_corruption(
                &db_path,
                &["docs_title_idx", "missing table", "missing_docs"],
            )
            .await;
        });
    }

    #[test]
    fn test_load_from_sqlite_rejects_malformed_explicit_index_sql() {
        asupersync::test_utils::run_test(|| async {
            let dir = tempfile::tempdir().unwrap();
            let db_path = dir.path().join("compat_malformed_explicit_index_sql.db");

            {
                let conn = rusqlite::Connection::open(&db_path).unwrap();
                conn.execute_batch(
                    "CREATE TABLE docs (id INTEGER PRIMARY KEY, title TEXT);
                     CREATE INDEX docs_title_idx ON docs(title);
                     PRAGMA writable_schema = ON;
                     UPDATE sqlite_master
                     SET sql = 'CREATE INDEX docs_title_idx ON'
                     WHERE type = 'index' AND name = 'docs_title_idx';
                     PRAGMA writable_schema = OFF;",
                )
                .unwrap();
            }

            assert_compat_rejects_catalog_corruption(
                &db_path,
                &["docs_title_idx", "could not be parsed"],
            )
            .await;
        });
    }

    #[test]
    fn test_load_from_sqlite_rejects_sqlite_schema_page_one_root_alias() {
        asupersync::test_utils::run_test(|| async {
            let dir = tempfile::tempdir().unwrap();
            let db_path = dir
                .path()
                .join("compat_sqlite_schema_page_one_root_alias.db");

            {
                let conn = rusqlite::Connection::open(&db_path).unwrap();
                conn.execute_batch(
                    "CREATE TABLE docs (id INTEGER PRIMARY KEY, title TEXT);
                     CREATE INDEX docs_title_idx ON docs(title);
                     PRAGMA writable_schema = ON;
                     UPDATE sqlite_master
                     SET rootpage = 1
                     WHERE type = 'index' AND name = 'docs_title_idx';
                     PRAGMA writable_schema = OFF;",
                )
                .unwrap();
            }

            let error = load_test_db(&db_path)
                .await
                .expect_err("compat loader must reject an index that claims sqlite_schema page 1");
            assert_schema_root_owner_collision(error, &["sqlite_schema", "docs_title_idx"]);
        });
    }

    #[test]
    fn test_load_from_sqlite_rejects_cross_table_root_alias() {
        asupersync::test_utils::run_test(|| async {
            let dir = tempfile::tempdir().unwrap();
            let db_path = dir.path().join("compat_cross_table_root_alias.db");

            {
                let conn = rusqlite::Connection::open(&db_path).unwrap();
                conn.execute_batch(
                    "CREATE TABLE alpha (id INTEGER PRIMARY KEY);
                     CREATE TABLE beta (id INTEGER PRIMARY KEY);
                     PRAGMA writable_schema = ON;",
                )
                .unwrap();
                let alpha_root = conn
                    .query_row(
                        "SELECT rootpage
                         FROM sqlite_schema
                         WHERE type = 'table' AND name = 'alpha';",
                        [],
                        |row| row.get::<_, i64>(0),
                    )
                    .unwrap();
                let changed = conn
                    .execute(
                        "UPDATE sqlite_master
                         SET rootpage = ?1
                         WHERE type = 'table' AND name = 'beta';",
                        [alpha_root],
                    )
                    .unwrap();
                assert_eq!(changed, 1);
                conn.execute_batch("PRAGMA writable_schema = OFF;").unwrap();
            }

            let error = load_test_db(&db_path)
                .await
                .expect_err("compat loader must reject cross-table root ownership aliases");
            assert_schema_root_owner_collision(error, &["alpha", "beta"]);
        });
    }

    #[test]
    fn test_load_from_sqlite_rejects_invalid_utf8_in_sqlite_master_record() {
        asupersync::test_utils::run_test(|| async {
            let dir = tempfile::tempdir().unwrap();
            let db_path = dir.path().join("compat_corrupt_master_utf8.db");

            {
                let conn = rusqlite::Connection::open(&db_path).unwrap();
                conn.execute_batch(
                    r"
                CREATE TABLE docs (id INTEGER PRIMARY KEY, title TEXT);
                INSERT INTO docs VALUES (1, 'hello');
                PRAGMA writable_schema = ON;
                UPDATE sqlite_master
                SET sql = CAST(x'FF' AS TEXT)
                WHERE name = 'docs';
                PRAGMA writable_schema = OFF;
                ",
                )
                .unwrap();
            }

            let err = load_test_db(&db_path)
                .await
                .expect_err("invalid sqlite_master text should fail");
            let message = err.to_string();
            assert!(
                message.contains("sqlite_master row")
                    || message.contains("valid SQLite record")
                    || message.contains("payload"),
                "unexpected load error: {message}"
            );
        });
    }

    #[test]
    fn test_load_from_sqlite_rejects_invalid_utf8_in_table_record() {
        asupersync::test_utils::run_test(|| async {
            let dir = tempfile::tempdir().unwrap();
            let db_path = dir.path().join("compat_corrupt_table_utf8.db");

            {
                let conn = rusqlite::Connection::open(&db_path).unwrap();
                conn.execute_batch(
                    r"
                CREATE TABLE docs (title TEXT);
                INSERT INTO docs VALUES (CAST(x'FF' AS TEXT));
                ",
                )
                .unwrap();
            }

            let err = load_test_db(&db_path)
                .await
                .expect_err("invalid table text should fail");
            let message = err.to_string();
            assert!(
                message.contains("table `docs`")
                    || message.contains("valid SQLite record")
                    || message.contains("payload"),
                "unexpected load error: {message}"
            );
        });
    }
}
