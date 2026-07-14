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
    ColumnConstraintKind, CreateTableBody, CreateTableStatement, DefaultValue, Expr,
    GeneratedStorage, IndexedColumn, Literal, SortDirection, Statement, TableConstraintKind,
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
use crate::connection::{eval_join_expr, is_sqlite_truthy};
use fsqlite_types::{DATABASE_HEADER_SIZE, DatabaseHeader, PageNumber, PageSize};
use fsqlite_vdbe::codegen::{ColumnInfo, FkActionType, FkDef, IndexSchema, TableSchema};
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
pub fn persist_to_sqlite(
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
    persist_to_sqlite_with_header(cx, path, schema, db, &header)
}

/// Persist `schema` + `db` using the provided database header template.
///
/// The supplied `header` controls page-size-sensitive layout plus header
/// metadata that must survive rebuild flows like `VACUUM`.
#[allow(clippy::too_many_lines)]
#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
pub fn persist_to_sqlite_with_header(
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
}

/// Persist `schema` + `db` plus additional sqlite_master rows using the
/// provided database header template.
#[allow(clippy::too_many_lines)]
#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
pub fn persist_to_sqlite_with_header_and_master_entries<S: BuildHasher>(
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
}

/// Persist into an atomically caller-reserved empty file.
///
/// The path is opened only through the pager's identity-bound `ReservedEmpty`
/// mode. A missing, replaced, non-empty, or sidecar-bearing reservation is
/// rejected before any database byte is initialized.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
pub fn persist_to_reserved_sqlite_with_header_and_master_entries<S: BuildHasher>(
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
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
fn persist_to_sqlite_with_header_and_master_entries_impl<S: BuildHasher>(
    cx: &Cx,
    path: &Path,
    schema: &[TableSchema],
    db: &MemDatabase,
    header_template: &DatabaseHeader,
    extra_master_entries: &[SqliteMasterEntry],
    original_ddl: &HashMap<String, String, S>,
    expected_empty_identity: Option<FileIdentity>,
) -> Result<()> {
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
        )?
    } else {
        SimplePager::open_with_cx(cx, vfs, path, header_template.page_size)?
    };
    let mut txn = pager.begin(cx, TransactionMode::Immediate)?;

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

        // Allocate a fresh root page for this table in the on-disk file.
        let root_page = txn.allocate_page(cx)?;

        // Initialize the root page as an empty leaf table B-tree.
        init_leaf_table_page(cx, &mut txn, root_page, page_size_usize, usable_size)?;

        // Insert all rows.
        {
            let mut cursor = fsqlite_btree::BtCursor::new(
                TransactionPageIo::new(&mut txn),
                root_page,
                usable_size,
                true,
            );
            configure_btree_cursor_page_size(&mut cursor, usable_size, full_page_size);
            for (rowid, values) in mem_table.iter_rows() {
                let payload = serialize_record(values);
                cursor.table_insert(cx, rowid, &payload)?;
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
        for index in &table.indexes {
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
            let idx_root = txn.allocate_page(cx)?;
            init_leaf_index_page(cx, &mut txn, idx_root, page_size_usize, usable_size)?;

            // Parse the partial index WHERE clause (if any) so we can skip
            // rows that don't satisfy the predicate.
            let partial_predicate = index
                .where_clause
                .as_deref()
                .map(fsqlite_parser::expr::parse_expr)
                .transpose()
                .ok()
                .flatten();

            // Populate the index B-tree from table rows.
            {
                let mut idx_cursor = fsqlite_btree::BtCursor::new(
                    TransactionPageIo::new(&mut txn),
                    idx_root,
                    usable_size,
                    true,
                );
                configure_btree_cursor_page_size(&mut idx_cursor, usable_size, full_page_size);
                if let Some(mem_table) = db.get_table(table.root_page) {
                    for (rowid, values) in mem_table.iter_rows() {
                        // For partial indexes, skip rows that don't match
                        // the WHERE predicate.
                        if let Some(ref predicate) = partial_predicate {
                            if let Ok(result) = eval_join_expr(predicate, values, &col_map) {
                                if !is_sqlite_truthy(&result) {
                                    continue;
                                }
                            }
                            // If evaluation fails, include the row (safe default).
                        }

                        // Build index key: (indexed_terms..., rowid).
                        let mut key_values: Vec<SqliteValue> = Vec::new();
                        if is_expression_index {
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
                        key_values.push(SqliteValue::Integer(rowid));
                        let key = serialize_record(&key_values);
                        idx_cursor.index_insert(cx, &key)?;
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
        let mut page1 = txn.get_page(cx, PageNumber::ONE)?.into_vec();
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
        txn.write_page(cx, PageNumber::ONE, &page1)?;

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
            cursor.table_insert(cx, rid, &record)?;
        }
    }

    // Fix up the database header on page 1: update page_count,
    // change_counter, and schema_cookie so sqlite3 validates the file.
    {
        let mut hdr_page = txn.get_page(cx, PageNumber::ONE)?.into_vec();

        // Discover the current page count by allocating one more page.
        // The extra page is included in the commit (the pager does not
        // support free_page), so the exported file has one trailing empty
        // page. This is benign: SQLite tolerates pages beyond the last
        // B-tree node, and the page_count header excludes it.
        let next_page = txn.allocate_page(cx)?.get();
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

        txn.write_page(cx, PageNumber::ONE, &hdr_page)?;
    }

    txn.commit(cx)?;
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
pub fn load_from_sqlite(cx: &Cx, path: &Path) -> Result<LoadedState> {
    let _record_profile_scope = enter_record_profile_scope(RecordProfileScope::CoreCompatPersist);
    let vfs = PlatformVfs::new();
    let pager = SimplePager::open_with_cx(cx, vfs, path, DEFAULT_PAGE_SIZE)?;
    let mut txn = pager.begin(cx, TransactionMode::ReadOnly)?;
    let page1 = txn.get_page(cx, PageNumber::ONE)?;
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

        if cursor.first(cx)? {
            let mut payload_buf: Vec<u8> = Vec::new();
            loop {
                // bd-9e3xf.6: fuse rowid+payload via the cursor accessor
                // landed in 5459d778 — saves one parse_cell_at per row on
                // the schema replay path, where N can be the full count of
                // sqlite_master rows in the database.
                payload_buf.clear();
                let rowid = cursor.rowid_and_payload_into(cx, &mut payload_buf)?;
                let values =
                    parse_record(&payload_buf).ok_or_else(|| FrankenError::DatabaseCorrupt {
                        detail: format!(
                            "sqlite_master row {rowid} payload is not a valid SQLite record"
                        ),
                    })?;
                entries.push(values);
                if !cursor.next(cx)? {
                    break;
                }
            }
        }
        entries
    };

    // Parse each sqlite_master row.
    // Columns: type(0), name(1), tbl_name(2), rootpage(3), sql(4)
    let materialized_virtual_tables: HashSet<String> = master_entries
        .iter()
        .filter_map(|entry| {
            if entry.len() < 5 {
                return None;
            }
            let entry_type = match &entry[0] {
                SqliteValue::Text(s) => s,
                _ => return None,
            };
            if !entry_type.eq_ignore_ascii_case("table") {
                return None;
            }
            let name = match &entry[1] {
                SqliteValue::Text(s) => s,
                _ => return None,
            };
            let root_page_num = match &entry[3] {
                SqliteValue::Integer(n) => *n,
                _ => return None,
            };
            let create_sql = match &entry[4] {
                SqliteValue::Text(s) => s,
                _ => return None,
            };
            if root_page_num > 0 && is_virtual_table_sql(create_sql) {
                Some(name.to_ascii_lowercase())
            } else {
                None
            }
        })
        .collect();
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
        if &**entry_type != "table" {
            continue; // Skip indexes, views, triggers for now.
        }

        let name = match &entry[1] {
            SqliteValue::Text(s) => s.clone(),
            _ => continue,
        };
        let root_page_num = match &entry[3] {
            SqliteValue::Integer(n) => *n,
            _ => continue,
        };
        let create_sql = match &entry[4] {
            SqliteValue::Text(s) => s.clone(),
            _ => continue,
        };

        // Stock SQLite records virtual tables with rootpage=0. Those legacy
        // declarations have no materialized root page to load, so skip them.
        // Positive-rootpage virtual tables are real B-trees and must remain
        // visible on reopen just like ordinary tables.
        if root_page_num == 0 && is_virtual_table_sql(&create_sql) {
            let _shadowed_by_materialized =
                materialized_virtual_tables.contains(&name.to_ascii_lowercase());
            continue;
        }
        let root_page_u32 = validate_sqlite_master_root_page(&name, root_page_num)?;

        // Parse the CREATE TABLE to extract column info and schema decorations.
        let columns = parse_columns_from_sqlite_master_sql(&create_sql);
        let indexes = extract_unique_constraint_indexes_from_sql(&create_sql, &name);
        let primary_key_constraints = extract_primary_key_constraints_from_sql(&create_sql);
        let foreign_keys = extract_foreign_keys_from_sql(&create_sql, &columns);
        let check_constraints = extract_check_constraints_from_sql(&create_sql);
        let num_columns = columns.len();
        let without_rowid = is_without_rowid_table_sql(&create_sql);
        let ipk_col_idx = columns.iter().position(|c| c.is_ipk);

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
            for (column_index, column) in current_table_schema.columns.iter().enumerate() {
                if column.unique && !column.is_ipk {
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
            if cursor.first(cx)? {
                if without_rowid {
                    let mut synthetic_rowid = 1_i64;
                    let mut payload_buf: Vec<u8> = Vec::new();
                    loop {
                        payload_buf.clear();
                        cursor.payload_into(cx, &mut payload_buf)?;
                        let mut values = parse_record(&payload_buf).ok_or_else(|| {
                            FrankenError::DatabaseCorrupt {
                                detail: format!(
                                    "WITHOUT ROWID table `{table_name_for_err}` payload is not a valid SQLite record"
                                ),
                            }
                        })?;
                        inflate_loaded_table_row_values(
                            &mut values,
                            synthetic_rowid,
                            &current_table_schema.columns,
                            None,
                            &table_name_for_err,
                        )?;
                        mem_table.insert_row(synthetic_rowid, values);
                        synthetic_rowid = synthetic_rowid.saturating_add(1);
                        if !cursor.next(cx)? {
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
                    let rowid = cursor.rowid_and_payload_into(cx, &mut payload_buf)?;
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
                    if !cursor.next(cx)? {
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
        if &**entry_type != "index" {
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
            _ => continue,
        };
        let create_sql = match &entry[4] {
            SqliteValue::Text(s) => s.to_string(),
            _ => continue,
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
            continue;
        };

        // Parse the CREATE INDEX SQL to extract column names, collations,
        // sort directions, and WHERE clause.
        if let Some(idx_schema) =
            self::parse_create_index_sql_to_schema(&index_name, root_page_i32, &create_sql)
        {
            // Only add if not already present (avoid duplicates with autoindexes).
            if !table.indexes.iter().any(|i| i.name == index_name) {
                table.indexes.push(idx_schema);
            }
        }
    }

    // Read schema_cookie and change_counter from the database header (page 1).
    let (schema_cookie, change_counter) = {
        let header_buf = txn.get_page(cx, PageNumber::ONE)?;
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
fn init_leaf_table_page(
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
    txn.write_page(cx, page_no, &page)
}

#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
fn init_leaf_index_page(
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
    txn.write_page(cx, page_no, &page)
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
    let primary_key_matches_index = |index: &fsqlite_vdbe::codegen::IndexSchema| {
        table.primary_key_constraints.iter().any(|pk| {
            pk.len() == index.columns.len()
                && pk
                    .iter()
                    .zip(index.columns.iter())
                    .all(|(lhs, rhs): (&String, &String)| lhs.eq_ignore_ascii_case(rhs))
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
            .iter()
            .map(|name| quote_identifier(name))
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
            .map(|name| quote_identifier(name))
            .collect::<Vec<_>>()
            .join(", ");
        let _ = write!(sql, ", UNIQUE ({cols})");
    }
    for fk in &table.foreign_keys {
        let child_columns = fk
            .child_columns
            .iter()
            .filter_map(|&column_index| table.columns.get(column_index))
            .map(|column| quote_identifier(&column.name))
            .collect::<Vec<_>>();
        if child_columns.is_empty() {
            continue;
        }
        let _ = write!(
            sql,
            ", FOREIGN KEY({}) REFERENCES {}",
            child_columns.join(", "),
            quote_identifier(&fk.parent_table)
        );
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
    }
    for check_expr in &table.check_constraints {
        let _ = write!(sql, ", CHECK({check_expr})");
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

pub(crate) fn extract_primary_key_constraints_from_sql(sql: &str) -> Vec<Vec<String>> {
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
        .filter(|column| {
            column.constraints.iter().any(|constraint| {
                matches!(constraint.kind, ColumnConstraintKind::PrimaryKey { .. })
            })
        })
        .map(|column| vec![column.name.clone()])
        .collect::<Vec<_>>();

    primary_keys.extend(constraints.iter().filter_map(|constraint| {
        let TableConstraintKind::PrimaryKey {
            columns: indexed_columns,
            ..
        } = &constraint.kind
        else {
            return None;
        };
        let columns = indexed_columns
            .iter()
            .filter_map(indexed_column_name)
            .map(str::to_owned)
            .collect::<Vec<_>>();
        (!columns.is_empty()).then_some(columns)
    }));

    primary_keys
}

fn extract_unique_constraint_indexes_from_sql(sql: &str, table_name: &str) -> Vec<IndexSchema> {
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

    let mut indexes = Vec::new();
    let mut autoindex_ordinal = 1_usize;

    for column in columns {
        let has_unique_constraint = column.constraints.iter().any(|constraint| {
            matches!(
                constraint.kind,
                ColumnConstraintKind::Unique { .. } | ColumnConstraintKind::PrimaryKey { .. }
            )
        });
        let is_ipk = column.type_name.as_ref().is_some_and(|type_name| {
            type_name.name.eq_ignore_ascii_case("INTEGER")
                && column.constraints.iter().any(|constraint| {
                    matches!(
                        constraint.kind,
                        ColumnConstraintKind::PrimaryKey {
                            direction: None | Some(SortDirection::Asc),
                            ..
                        }
                    )
                })
        });
        if has_unique_constraint && !is_ipk {
            indexes.push(IndexSchema {
                name: format!("sqlite_autoindex_{table_name}_{autoindex_ordinal}"),
                root_page: 0,
                columns: vec![column.name.clone()],
                key_expressions: Vec::new(),
                key_sort_directions: vec![SortDirection::Asc],
                where_clause: None,
                is_unique: true,
                key_collations: vec![column.constraints.iter().find_map(|constraint| {
                    if let ColumnConstraintKind::Collate(name) = &constraint.kind {
                        Some(name.clone())
                    } else {
                        None
                    }
                })],
                conflict_action: column
                    .constraints
                    .iter()
                    .find_map(|constraint| match &constraint.kind {
                        ColumnConstraintKind::Unique { conflict }
                        | ColumnConstraintKind::PrimaryKey { conflict, .. } => *conflict,
                        _ => None,
                    }),
            });
            autoindex_ordinal += 1;
        }
    }

    for constraint in constraints {
        let (indexed_columns, is_primary_key) = match &constraint.kind {
            TableConstraintKind::Unique {
                columns: indexed_columns,
                ..
            } => (indexed_columns, false),
            TableConstraintKind::PrimaryKey {
                columns: indexed_columns,
                ..
            } => (indexed_columns, true),
            _ => continue,
        };
        if is_primary_key
            && table_primary_key_is_rowid_alias(columns, indexed_columns, create.without_rowid)
        {
            continue;
        }
        let Some(normalized_terms) = indexed_columns
            .iter()
            .map(|indexed_column| {
                Some((
                    indexed_column_name(indexed_column)?.to_owned(),
                    indexed_column_collation(indexed_column),
                ))
            })
            .collect::<Option<Vec<_>>>()
        else {
            continue;
        };
        let columns = normalized_terms
            .iter()
            .map(|(column_name, _)| column_name.clone())
            .collect::<Vec<_>>();
        if columns.is_empty() {
            continue;
        }
        indexes.push(IndexSchema {
            name: format!("sqlite_autoindex_{table_name}_{autoindex_ordinal}"),
            root_page: 0,
            columns,
            key_expressions: Vec::new(),
            key_sort_directions: indexed_columns
                .iter()
                .map(|indexed| indexed.direction.unwrap_or(SortDirection::Asc))
                .collect(),
            where_clause: None,
            is_unique: true,
            key_collations: normalized_terms
                .into_iter()
                .map(|(_, collation)| collation)
                .collect(),
            conflict_action: match &constraint.kind {
                TableConstraintKind::Unique { conflict, .. }
                | TableConstraintKind::PrimaryKey { conflict, .. } => *conflict,
                _ => None,
            },
        });
        autoindex_ordinal += 1;
    }

    indexes
}

fn extract_foreign_keys_from_sql(sql: &str, columns: &[ColumnInfo]) -> Vec<FkDef> {
    let Some(Statement::CreateTable(create)) = parse_single_statement(sql) else {
        return Vec::new();
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
                foreign_keys.push(fk_clause_to_def(&[column_index], clause));
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
                foreign_keys.push(fk_clause_to_def(&child_indices, clause));
            }
        }
    }

    foreign_keys
}

fn fk_clause_to_def(child_indices: &[usize], clause: &fsqlite_ast::ForeignKeyClause) -> FkDef {
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
    let Some(open) = sql.find('(') else {
        return Vec::new();
    };
    let Some(close) = sql.rfind(')') else {
        return Vec::new();
    };
    if open >= close {
        return Vec::new();
    }

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

            Some(ColumnInfo {
                name,
                affinity,
                is_ipk,
                type_name,
                notnull: has_not_null,
                unique: has_unique || has_primary_key,
                default_value,
                strict_type,
                generated_expr: None,
                generated_stored: None,
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
    if let Some(Statement::CreateTable(create)) = parse_single_statement(sql) {
        return check_constraints_from_create_table_statement(&create);
    }

    let Some(open) = sql.find('(') else {
        return Vec::new();
    };
    let Some(close) = sql.rfind(')') else {
        return Vec::new();
    };
    if open >= close {
        return Vec::new();
    }
    let body = &sql[open + 1..close];
    let upper = body.to_ascii_uppercase();
    let mut checks = Vec::new();
    let mut search_from = 0;
    while let Some(pos) = upper[search_from..].find("CHECK") {
        let abs_pos = search_from + pos;
        let after = &body[abs_pos + 5..].trim_start();
        if after.starts_with('(') {
            // Find matching closing paren.
            let mut depth = 0_i32;
            let mut end = None;
            for (i, ch) in after.char_indices() {
                match ch {
                    '(' => depth += 1,
                    ')' => {
                        depth -= 1;
                        if depth == 0 {
                            end = Some(i);
                            break;
                        }
                    }
                    _ => {}
                }
            }
            if let Some(end_idx) = end {
                let expr = &after[1..end_idx];
                checks.push(expr.trim().to_owned());
                search_from = abs_pos + 5 + end_idx + 1;
            } else {
                search_from = abs_pos + 5;
            }
        } else {
            search_from = abs_pos + 5;
        }
    }
    checks
}

pub(crate) fn check_constraints_from_create_table_statement(
    create: &CreateTableStatement,
) -> Vec<String> {
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
                checks.push(expr.to_string());
            }
        }
    }
    for constraint in constraints {
        if let TableConstraintKind::Check(expr) = &constraint.kind {
            checks.push(expr.to_string());
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

fn table_primary_key_is_rowid_alias(
    columns: &[fsqlite_ast::ColumnDef],
    indexed_columns: &[IndexedColumn],
    without_rowid: bool,
) -> bool {
    if without_rowid || indexed_columns.len() != 1 {
        return false;
    }
    let Some(column_name) = indexed_column_name(&indexed_columns[0]) else {
        return false;
    };
    columns
        .iter()
        .find(|column| column.name.eq_ignore_ascii_case(column_name))
        .and_then(|column| column.type_name.as_ref())
        .is_some_and(|type_name| type_name.name.eq_ignore_ascii_case("INTEGER"))
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

    let mut table_pk_cols = vec![false; columns.len()];
    let mut table_unique_cols = vec![false; columns.len()];
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

                    table_pk_cols[index] = true;
                    table_unique_cols[index] = true;

                    let is_integer = columns[index]
                        .type_name
                        .as_ref()
                        .is_some_and(|tn| tn.name.eq_ignore_ascii_case("INTEGER"));
                    if is_integer && !create.without_rowid {
                        table_pk_rowid_col_idx = Some(index);
                    }
                }
                TableConstraintKind::Unique {
                    columns: unique_columns,
                    ..
                } if unique_columns.len() == 1 => {
                    let Some(column_name) = indexed_column_name(&unique_columns[0]) else {
                        continue;
                    };
                    let Some(index) = columns
                        .iter()
                        .position(|col| col.name.eq_ignore_ascii_case(column_name))
                    else {
                        continue;
                    };
                    table_unique_cols[index] = true;
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
                    || table_pk_cols[index]
                    || table_unique_cols[index]
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
    let trimmed = def.trim_start();
    if trimmed.is_empty() {
        return false;
    }
    match trimmed.as_bytes()[0] {
        b'"' | b'`' | b'[' => return false,
        _ => {}
    }
    let upper = trimmed.to_ascii_uppercase();
    upper.starts_with("CONSTRAINT ")
        || upper.starts_with("PRIMARY KEY")
        || upper == "PRIMARY"
        || upper.starts_with("UNIQUE ")
        || upper.starts_with("UNIQUE(")
        || upper == "UNIQUE"
        || upper.starts_with("CHECK ")
        || upper.starts_with("CHECK(")
        || upper == "CHECK"
        || upper.starts_with("FOREIGN KEY")
        || upper.starts_with("FOREIGN(")
        || upper == "FOREIGN"
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

    fn persist_test_db(
        path: &Path,
        schema: &[TableSchema],
        db: &MemDatabase,
        schema_cookie: u32,
        change_counter: u32,
    ) -> Result<()> {
        let cx = Cx::new();
        persist_to_sqlite(&cx, path, schema, db, schema_cookie, change_counter)
    }

    fn load_test_db(path: &Path) -> Result<LoadedState> {
        let cx = Cx::new();
        load_from_sqlite(&cx, path)
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
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        let (schema, db) = make_test_schema_and_db();
        persist_test_db(&db_path, &schema, &db, 0, 0).unwrap();

        assert!(db_path.exists(), "db file should exist");
        assert!(is_sqlite_format(&db_path), "should have SQLite magic");

        let loaded = load_test_db(&db_path).unwrap();
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
    }

    #[test]
    fn test_empty_database_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("empty.db");

        let schema: Vec<TableSchema> = Vec::new();
        let db = MemDatabase::new();
        persist_test_db(&db_path, &schema, &db, 0, 0).unwrap();

        assert!(is_sqlite_format(&db_path));

        let loaded = load_test_db(&db_path).unwrap();
        assert!(loaded.schema.is_empty());
    }

    #[test]
    fn test_persist_creates_sqlite3_readable_file() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("readable.db");

        let (schema, db) = make_test_schema_and_db();
        persist_test_db(&db_path, &schema, &db, 0, 0).unwrap();

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
        let loaded = load_test_db(&db_path).unwrap();
        assert_eq!(loaded.schema.len(), 1);
        assert_eq!(loaded.schema[0].name, "items");

        let table = loaded.db.get_table(loaded.schema[0].root_page).unwrap();
        let rows: Vec<_> = table.iter_rows().collect();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].1[0], SqliteValue::Integer(10));
        assert_eq!(rows[0].1[1], SqliteValue::Text("alpha".into()));
        assert_eq!(rows[1].1[0], SqliteValue::Integer(20));
        assert_eq!(rows[1].1[1], SqliteValue::Text("beta".into()));
    }

    #[test]
    fn test_load_sqlite3_created_file_restores_integer_primary_key_alias_values() {
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

        let loaded = load_test_db(&db_path).unwrap();
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
    }

    #[test]
    fn test_load_sqlite3_created_file_restores_table_level_integer_primary_key_alias_values() {
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

        let loaded = load_test_db(&db_path).unwrap();
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
    }

    #[test]
    fn test_load_sqlite3_rowid_alias_multi_alter_short_rows_preserves_alignment() {
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

        let loaded = load_test_db(&db_path).unwrap();
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
    }

    #[test]
    fn test_load_sqlite3_rowid_alias_parenthesized_added_defaults() {
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

        let loaded = load_test_db(&db_path).unwrap();
        let table = loaded.db.get_table(loaded.schema[0].root_page).unwrap();
        let rows: Vec<_> = table.iter_rows().collect();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, 3);
        assert_eq!(rows[0].1[0], SqliteValue::Integer(3));
        assert_eq!(rows[0].1[1], SqliteValue::Text("alpha".into()));
        assert_eq!(rows[0].1[2], SqliteValue::Integer(9));
        assert_eq!(rows[0].1[3], SqliteValue::Text("fallback".into()));
    }

    #[test]
    fn test_load_sqlite3_altered_short_rows_parse_boolean_blob_and_quoted_defaults() {
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

        let loaded = load_test_db(&db_path).unwrap();
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

        let loaded = load_test_db(&db_path).unwrap_or_else(|error| {
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

        persist_test_db(&db_path, &schema, &db, 0, 0).unwrap();
        let loaded = load_test_db(&db_path).unwrap();

        assert_eq!(loaded.schema.len(), 2);
        assert_eq!(loaded.schema[0].name, "alpha");
        assert_eq!(loaded.schema[1].name, "beta");

        let tbl_a = loaded.db.get_table(loaded.schema[0].root_page).unwrap();
        let rows_a: Vec<_> = tbl_a.iter_rows().collect();
        assert_eq!(rows_a[0].1[0], SqliteValue::Text("row_a".into()));

        let tbl_b = loaded.db.get_table(loaded.schema[1].root_page).unwrap();
        let rows_b: Vec<_> = tbl_b.iter_rows().collect();
        assert_eq!(rows_b[0].1[0], SqliteValue::Integer(777));
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
            primary_key_constraints: vec![vec!["id".to_owned()]],
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
                parent_table: "parent".to_owned(),
                parent_columns: vec!["id".to_owned()],
                on_delete: FkActionType::Cascade,
                on_update: FkActionType::Restrict,
                deferred: false,
            }],
            check_constraints: vec!["length(slug) > 0".to_owned()],
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
        );
        assert_eq!(indexes.len(), 1);
        assert_eq!(indexes[0].columns, vec!["tenant", "slug"]);
        assert!(indexes[0].is_unique);
    }

    #[test]
    fn test_extract_unique_constraint_indexes_skips_table_level_integer_primary_key_alias() {
        let indexes = extract_unique_constraint_indexes_from_sql(
            "CREATE TABLE metrics (id INTEGER, body TEXT, PRIMARY KEY(id COLLATE NOCASE DESC))",
            "metrics",
        );
        assert!(indexes.is_empty(), "{indexes:?}");
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

        let loaded = load_test_db(&source_path).unwrap();
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

        let reopened = load_test_db(&rebuilt_path).unwrap();
        let table = reopened
            .schema
            .iter()
            .find(|table| table.name == "link_table")
            .unwrap();
        assert!(table.indexes.iter().any(|index| index.name == INDEX_NAME));
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
            primary_key_constraints: vec![vec!["id".to_owned()]],
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
    }

    #[test]
    fn test_overwrite_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("overwrite.db");

        // Write once.
        let (schema, db) = make_test_schema_and_db();
        persist_test_db(&db_path, &schema, &db, 0, 0).unwrap();

        // Overwrite with empty.
        persist_test_db(&db_path, &[], &MemDatabase::new(), 0, 0).unwrap();

        let loaded = load_test_db(&db_path).unwrap();
        assert!(loaded.schema.is_empty());
    }

    #[test]
    fn test_load_from_sqlite_keeps_materialized_virtual_tables_with_real_root_page() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("materialized_vtab_load.db");
        let db_str = db_path.to_string_lossy().to_string();

        {
            let conn = crate::connection::Connection::open(&db_str).unwrap();
            conn.execute("CREATE VIRTUAL TABLE docs USING fts5(subject, body, tokenize='porter')")
                .unwrap();
            conn.execute(
                "INSERT INTO docs(rowid, subject, body) VALUES (1, 'Hello', 'Rust world')",
            )
            .unwrap();
            conn.execute("INSERT INTO docs(rowid, subject, body) VALUES (2, 'Other', 'Nothing')")
                .unwrap();
            conn.close().unwrap();
        }

        let loaded = load_test_db(&db_path).unwrap();
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
    }

    #[test]
    fn test_load_from_sqlite_rejects_non_virtual_table_with_rootpage_zero() {
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

        let err = match load_test_db(&db_path) {
            Ok(_) => panic!("corrupt rootpage should fail load"),
            Err(err) => err,
        };
        let message = err.to_string();
        assert!(
            message.contains("rootpage 0") || message.contains("root page"),
            "unexpected load error: {message}"
        );
    }

    #[test]
    fn test_load_from_sqlite_rejects_negative_rootpage() {
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

        let err = match load_test_db(&db_path) {
            Ok(_) => panic!("negative rootpage should fail load"),
            Err(err) => err,
        };
        let message = err.to_string();
        assert!(
            message.contains("rootpage -7") || message.contains("invalid rootpage"),
            "unexpected load error: {message}"
        );
    }

    #[test]
    fn test_load_from_sqlite_rejects_rootpage_above_supported_range() {
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

        let err = match load_test_db(&db_path) {
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
    }

    #[test]
    fn test_load_from_sqlite_rejects_index_rootpage_above_supported_range() {
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

        let err = match load_test_db(&db_path) {
            Ok(_) => panic!("oversized index rootpage should fail load"),
            Err(err) => err,
        };
        let message = err.to_string();
        assert!(
            message.contains("docs_title_idx") && message.contains("2147483648"),
            "unexpected load error: {message}"
        );
    }

    #[test]
    fn test_load_from_sqlite_rejects_invalid_utf8_in_sqlite_master_record() {
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

        let err = load_test_db(&db_path).expect_err("invalid sqlite_master text should fail");
        let message = err.to_string();
        assert!(
            message.contains("sqlite_master row")
                || message.contains("valid SQLite record")
                || message.contains("payload"),
            "unexpected load error: {message}"
        );
    }

    #[test]
    fn test_load_from_sqlite_rejects_invalid_utf8_in_table_record() {
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

        let err = load_test_db(&db_path).expect_err("invalid table text should fail");
        let message = err.to_string();
        assert!(
            message.contains("table `docs`")
                || message.contains("valid SQLite record")
                || message.contains("payload"),
            "unexpected load error: {message}"
        );
    }
}
