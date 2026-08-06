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

use std::collections::{HashMap, HashSet};
#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
use std::hash::BuildHasher;
#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
use std::path::Path;

use fsqlite_ast::{
    ColumnConstraintKind, CreateTableBody, CreateTableStatement, DefaultValue, Expr,
    GeneratedStorage, Literal, SortDirection, Statement, TableConstraintKind, TriggerTiming,
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

use crate::connection::{
    ImplicitAutoindexSlot, column_def_is_exact_integer, implicit_autoindex_layout,
    validate_builtin_persisted_index_expr_functions,
};
#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
use crate::connection::{eval_join_expr, is_sqlite_truthy};
use fsqlite_types::{DATABASE_HEADER_SIZE, DatabaseHeader, PageNumber, PageSize};
use fsqlite_vdbe::codegen::{
    CheckConstraint, ColumnInfo, FkActionType, FkDef, IndexSchema, TableSchema, bind_explicit_index,
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
    suffix.parse::<usize>().ok()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BoundImplicitAutoindexStorage {
    /// The logical WITHOUT ROWID primary-key index is stored in the table
    /// B-tree itself and therefore has no separate sqlite_master row.
    TableRoot,
    /// A physical implicit index backed by its own B-tree root.
    IndexRoot(i32),
}

#[derive(Debug, Clone)]
pub(crate) struct BoundImplicitAutoindexSlot {
    ordinal: usize,
    slot: ImplicitAutoindexSlot,
    storage: BoundImplicitAutoindexStorage,
}

#[derive(Debug, Clone)]
pub(crate) struct BoundTableAutoindexes {
    slots: Vec<BoundImplicitAutoindexSlot>,
}

impl BoundTableAutoindexes {
    pub(crate) fn implicit_slots(&self) -> impl Iterator<Item = &ImplicitAutoindexSlot> {
        self.slots.iter().map(|bound| &bound.slot)
    }

    pub(crate) fn physical_index_schemas(&self, table_name: &str) -> Vec<IndexSchema> {
        self.slots
            .iter()
            .filter_map(|bound| match bound.storage {
                BoundImplicitAutoindexStorage::TableRoot => None,
                BoundImplicitAutoindexStorage::IndexRoot(root_page) => Some(
                    bound
                        .slot
                        .clone()
                        .into_index_schema(table_name, bound.ordinal, root_page),
                ),
            })
            .collect()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct BoundImplicitAutoindexCatalog {
    by_table: HashMap<String, BoundTableAutoindexes>,
    canonical_virtual_table_rows: HashSet<usize>,
}

impl BoundImplicitAutoindexCatalog {
    pub(crate) fn table(&self, table_name: &str) -> Option<&BoundTableAutoindexes> {
        self.by_table.get(&table_name.to_ascii_lowercase())
    }

    pub(crate) fn is_canonical_virtual_table_row(&self, row_index: usize) -> bool {
        self.canonical_virtual_table_rows.contains(&row_index)
    }
}

#[derive(Debug, Clone, Copy)]
struct DecodedSqliteMasterEntry<'a> {
    entry_type: &'a str,
    name: &'a str,
    table_name: &'a str,
    root_page: i64,
    sql: Option<&'a str>,
}

#[derive(Debug)]
struct PendingTableAutoindexes {
    table_name: String,
    slots: Vec<ImplicitAutoindexSlot>,
    physical_roots: Vec<Option<i32>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SqliteMasterSchemaNameKind {
    Table,
    VirtualTable,
    View,
    Index,
}

impl SqliteMasterSchemaNameKind {
    const fn label(self) -> &'static str {
        match self {
            Self::Table => "table",
            Self::VirtualTable => "virtual table",
            Self::View => "view",
            Self::Index => "index",
        }
    }
}

#[derive(Debug)]
struct SqliteMasterSchemaNameOwner {
    name: String,
    kind: SqliteMasterSchemaNameKind,
}

#[derive(Debug)]
struct VirtualTableCatalogVariant {
    row_index: usize,
    name: String,
    root_page: i64,
    module: String,
    args: Vec<String>,
}

fn sqlite_master_corrupt(detail: impl Into<String>) -> FrankenError {
    FrankenError::DatabaseCorrupt {
        detail: detail.into(),
    }
}

fn qualified_catalog_name_targets_main(schema: Option<&str>) -> bool {
    schema.is_none_or(|schema| schema.eq_ignore_ascii_case("main"))
}

fn claim_sqlite_master_schema_name(
    name: &str,
    kind: SqliteMasterSchemaNameKind,
    schema_names: &mut HashMap<String, SqliteMasterSchemaNameOwner>,
) -> Result<()> {
    let key = name.to_ascii_lowercase();
    if let Some(existing) = schema_names.get(&key) {
        return Err(sqlite_master_corrupt(format!(
            "sqlite_master schema name `{name}` is shared by {} `{}` and {} `{name}`",
            existing.kind.label(),
            existing.name,
            kind.label()
        )));
    }
    schema_names.insert(
        key,
        SqliteMasterSchemaNameOwner {
            name: name.to_owned(),
            kind,
        },
    );
    Ok(())
}

fn normalize_virtual_table_option_token(token: &str) -> String {
    let trimmed = token.trim();
    if trimmed.len() >= 2 {
        let bytes = trimmed.as_bytes();
        let first = bytes[0];
        let last = bytes[bytes.len() - 1];
        if matches!((first, last), (b'\'', b'\'') | (b'"', b'"') | (b'`', b'`')) {
            let body = &trimmed[1..trimmed.len() - 1];
            let quote = char::from(first);
            return body
                .replace(&format!("{quote}{quote}"), &quote.to_string())
                .to_ascii_lowercase();
        }
        if first == b'[' && last == b']' {
            return trimmed[1..trimmed.len() - 1].to_ascii_lowercase();
        }
    }
    trimmed.to_ascii_lowercase()
}

fn fts5_content_variant(args: &[String]) -> Option<(Option<String>, Vec<&str>)> {
    let mut content = None;
    let mut non_content = Vec::with_capacity(args.len());
    for arg in args {
        let is_content_option = arg
            .split_once('=')
            .is_some_and(|(key, _)| normalize_virtual_table_option_token(key) == "content");
        if is_content_option {
            let (_, value) = arg
                .split_once('=')
                .expect("content option was identified by an equals sign");
            if content
                .replace(normalize_virtual_table_option_token(value))
                .is_some()
            {
                return None;
            }
        } else {
            non_content.push(arg.trim());
        }
    }
    Some((content, non_content))
}

fn supported_virtual_table_duplicate(
    existing: &VirtualTableCatalogVariant,
    root_page: i64,
    module: &str,
    args: &[String],
) -> bool {
    // Two positive roots are rejected by the caller. An otherwise identical
    // positive/root-zero pair is the legacy materialized-vtab migration shape;
    // two root-zero rows are accepted only for FTS5's repair path.
    if existing.module.eq_ignore_ascii_case(module) && existing.args == args {
        return existing.root_page > 0 || root_page > 0 || module.eq_ignore_ascii_case("fts5");
    }
    if existing.root_page != 0
        || root_page != 0
        || !existing.module.eq_ignore_ascii_case("fts5")
        || !module.eq_ignore_ascii_case("fts5")
    {
        return false;
    }
    // A historical FTS5 repair can leave the authoritative contentless row
    // beside a stale default-content declaration. The non-content arguments
    // must still be identical, and no third catalog row is accepted.
    let Some((existing_content, existing_non_content)) = fts5_content_variant(&existing.args)
    else {
        return false;
    };
    let Some((candidate_content, candidate_non_content)) = fts5_content_variant(args) else {
        return false;
    };
    existing_non_content == candidate_non_content
        && matches!(
            (existing_content.as_deref(), candidate_content.as_deref()),
            (None, Some("")) | (Some(""), None)
        )
}

fn canonical_virtual_table_variant(
    variants: &[VirtualTableCatalogVariant],
) -> &VirtualTableCatalogVariant {
    variants
        .iter()
        .find(|variant| variant.root_page > 0)
        .or_else(|| {
            variants.iter().find(|variant| {
                variant.module.eq_ignore_ascii_case("fts5")
                    && fts5_content_variant(&variant.args)
                        .is_some_and(|(content, _)| content.as_deref() == Some(""))
            })
        })
        .unwrap_or_else(|| {
            variants
                .first()
                .expect("validated virtual-table variant set is non-empty")
        })
}

fn claim_sqlite_master_virtual_table(
    row_index: usize,
    name: &str,
    root_page: i64,
    module: &str,
    args: &[String],
    schema_names: &mut HashMap<String, SqliteMasterSchemaNameOwner>,
    variants: &mut HashMap<String, Vec<VirtualTableCatalogVariant>>,
) -> Result<()> {
    let key = name.to_ascii_lowercase();
    if let Some(existing_variants) = variants.get_mut(&key) {
        let Some(existing) = existing_variants.first() else {
            unreachable!("virtual-table variant map entries are never empty");
        };
        if existing_variants.len() != 1
            || (existing.root_page > 0 && root_page > 0)
            || !supported_virtual_table_duplicate(existing, root_page, module, args)
        {
            return Err(sqlite_master_corrupt(format!(
                "sqlite_master contains conflicting virtual-table entries for `{}` and `{name}`",
                existing.name
            )));
        }
        existing_variants.push(VirtualTableCatalogVariant {
            row_index,
            name: name.to_owned(),
            root_page,
            module: module.to_owned(),
            args: args.to_vec(),
        });
        return Ok(());
    }

    claim_sqlite_master_schema_name(name, SqliteMasterSchemaNameKind::VirtualTable, schema_names)?;
    variants.insert(
        key,
        vec![VirtualTableCatalogVariant {
            row_index,
            name: name.to_owned(),
            root_page,
            module: module.to_owned(),
            args: args.to_vec(),
        }],
    );
    Ok(())
}

fn decode_sqlite_master_entry(
    entry: &[SqliteValue],
    row_index: usize,
) -> Result<DecodedSqliteMasterEntry<'_>> {
    if entry.len() != 5 {
        return Err(sqlite_master_corrupt(format!(
            "sqlite_master row {} has {} columns instead of 5",
            row_index + 1,
            entry.len()
        )));
    }
    let text_column = |column_index: usize, column_name: &str| match &entry[column_index] {
        SqliteValue::Text(value) => Ok(value.as_ref()),
        value => Err(sqlite_master_corrupt(format!(
            "sqlite_master row {} column `{column_name}` must be TEXT, found {value:?}",
            row_index + 1
        ))),
    };
    let entry_type = text_column(0, "type")?;
    let name = text_column(1, "name")?;
    let table_name = text_column(2, "tbl_name")?;
    let root_page = match &entry[3] {
        SqliteValue::Integer(value) => *value,
        value => {
            return Err(sqlite_master_corrupt(format!(
                "sqlite_master row {} column `rootpage` must be INTEGER, found {value:?}",
                row_index + 1
            )));
        }
    };
    let sql = match &entry[4] {
        SqliteValue::Text(value) => Some(value.as_ref()),
        SqliteValue::Null => None,
        value => {
            return Err(sqlite_master_corrupt(format!(
                "sqlite_master row {} column `sql` must be TEXT or NULL, found {value:?}",
                row_index + 1
            )));
        }
    };
    Ok(DecodedSqliteMasterEntry {
        entry_type,
        name,
        table_name,
        root_page,
        sql,
    })
}

fn claim_sqlite_master_root(
    entry_kind: &str,
    entry_name: &str,
    root_page: i64,
    max_root_page: u32,
    header: &DatabaseHeader,
    free_pages: &HashSet<PageNumber>,
    root_owners: &mut HashMap<i32, String>,
) -> Result<i32> {
    let root_page_u32 = validate_sqlite_master_root_page(entry_name, root_page)?;
    let root_page_i32 = i32::try_from(root_page_u32).map_err(|_| {
        sqlite_master_corrupt(format!(
            "sqlite_master {entry_kind} `{entry_name}` has unsupported rootpage {root_page}"
        ))
    })?;
    if root_page_i32 >= i32::MAX - 1 {
        return Err(sqlite_master_corrupt(format!(
            "sqlite_master {entry_kind} `{entry_name}` has terminal rootpage {root_page}, which leaves no safe MemDatabase allocation sentinel"
        )));
    }
    if root_page_u32 > max_root_page {
        return Err(sqlite_master_corrupt(format!(
            "sqlite_master {entry_kind} `{entry_name}` has rootpage {root_page}, which exceeds the visible database page count {max_root_page}"
        )));
    }
    if root_page_u32 == fsqlite_pager::lock_byte_page(header.page_size) {
        return Err(sqlite_master_corrupt(format!(
            "sqlite_master {entry_kind} `{entry_name}` uses reserved lock-byte rootpage {root_page}"
        )));
    }
    let root_page_number =
        PageNumber::new(root_page_u32).expect("validated positive rootpage must be nonzero");
    if header.largest_root_page != 0
        && fsqlite_btree::freelist::is_ptrmap_page(
            root_page_number,
            header.page_size.usable(header.reserved_per_page),
            header.page_size.get(),
        )
    {
        return Err(sqlite_master_corrupt(format!(
            "sqlite_master {entry_kind} `{entry_name}` uses auto-vacuum pointer-map rootpage {root_page}"
        )));
    }
    if free_pages.contains(&root_page_number) {
        return Err(sqlite_master_corrupt(format!(
            "sqlite_master {entry_kind} `{entry_name}` uses free rootpage {root_page}"
        )));
    }
    let owner = format!("{entry_kind} `{entry_name}`");
    if let Some(existing_owner) = root_owners.insert(root_page_i32, owner.clone()) {
        return Err(sqlite_master_corrupt(format!(
            "sqlite_master rootpage {root_page_i32} is shared by {existing_owner} and {owner}"
        )));
    }
    Ok(root_page_i32)
}

/// Validate and bind every implicit autoindex row in a sqlite_master snapshot.
///
/// This is deliberately a global, failure-atomic prepass. Both schema reload
/// paths must prove the complete expected implicit-index set and root ownership
/// before either mutates `MemDatabase`, because `create_table_at` replaces an
/// existing root on collision.
pub(crate) fn bind_implicit_autoindex_catalog(
    master_entries: &[Vec<SqliteValue>],
    max_root_page: u32,
    header: &DatabaseHeader,
    free_pages: &HashSet<PageNumber>,
) -> Result<BoundImplicitAutoindexCatalog> {
    if max_root_page == 0 {
        return Err(sqlite_master_corrupt(
            "sqlite_master cannot be bound without a visible database page",
        ));
    }
    let decoded = master_entries
        .iter()
        .enumerate()
        .map(|(row_index, entry)| decode_sqlite_master_entry(entry, row_index))
        .collect::<Result<Vec<_>>>()?;
    let mut root_owners = HashMap::new();
    root_owners.insert(1, "sqlite_master".to_owned());
    let mut pending_by_table = HashMap::<String, PendingTableAutoindexes>::new();
    let mut schema_names = HashMap::<String, SqliteMasterSchemaNameOwner>::new();
    let mut trigger_names = HashMap::<String, String>::new();
    let mut virtual_table_variants = HashMap::<String, Vec<VirtualTableCatalogVariant>>::new();
    let mut logical_autoindex_names = HashMap::<String, String>::new();
    let mut pending_trigger_parents = Vec::<(String, String, bool)>::new();

    // Claim every table root before any index root, independent of catalog row
    // order, so an index can never replace a table placeholder during reload.
    for (row_index, entry) in decoded.iter().enumerate() {
        if entry.entry_type.eq_ignore_ascii_case("view") {
            if entry.root_page != 0 || entry.sql.is_none() {
                return Err(sqlite_master_corrupt(format!(
                    "sqlite_master view `{}` must have rootpage 0 and non-NULL sql",
                    entry.name
                )));
            }
            if !entry.name.eq_ignore_ascii_case(entry.table_name) {
                return Err(sqlite_master_corrupt(format!(
                    "sqlite_master view `{}` has mismatched tbl_name `{}`",
                    entry.name, entry.table_name
                )));
            }
            let create_sql = entry.sql.expect("view sql was validated above");
            let Some(Statement::CreateView(create)) = parse_single_statement(create_sql) else {
                return Err(sqlite_master_corrupt(format!(
                    "could not parse CREATE VIEW SQL for `{}`",
                    entry.name
                )));
            };
            if create.temporary
                || !qualified_catalog_name_targets_main(create.name.schema.as_deref())
                || !create.name.name.eq_ignore_ascii_case(entry.name)
            {
                return Err(sqlite_master_corrupt(format!(
                    "CREATE VIEW SQL for `{}` declares a temporary, non-main, or differently named view `{}`",
                    entry.name, create.name.name
                )));
            }
            claim_sqlite_master_schema_name(
                entry.name,
                SqliteMasterSchemaNameKind::View,
                &mut schema_names,
            )?;
            continue;
        }
        if entry.entry_type.eq_ignore_ascii_case("trigger") {
            if entry.root_page != 0 || entry.sql.is_none() {
                return Err(sqlite_master_corrupt(format!(
                    "sqlite_master trigger `{}` must have rootpage 0 and non-NULL sql",
                    entry.name
                )));
            }
            let create_sql = entry.sql.expect("trigger sql was validated above");
            let Some(Statement::CreateTrigger(create)) = parse_single_statement(create_sql) else {
                return Err(sqlite_master_corrupt(format!(
                    "could not parse CREATE TRIGGER SQL for `{}`",
                    entry.name
                )));
            };
            if create.temporary
                || !qualified_catalog_name_targets_main(create.name.schema.as_deref())
                || !create.name.name.eq_ignore_ascii_case(entry.name)
                || !create.table.eq_ignore_ascii_case(entry.table_name)
            {
                return Err(sqlite_master_corrupt(format!(
                    "CREATE TRIGGER SQL for `{}` does not match its main-catalog name or target `{}`",
                    entry.name, entry.table_name
                )));
            }
            let trigger_key = entry.name.to_ascii_lowercase();
            if let Some(existing) = trigger_names.insert(trigger_key, entry.name.to_owned()) {
                return Err(sqlite_master_corrupt(format!(
                    "sqlite_master contains duplicate trigger entries for `{existing}` and `{}`",
                    entry.name
                )));
            }
            pending_trigger_parents.push((
                entry.name.to_owned(),
                entry.table_name.to_owned(),
                matches!(create.timing, TriggerTiming::InsteadOf),
            ));
            continue;
        }
        if entry.entry_type.eq_ignore_ascii_case("index") {
            continue;
        }
        if !entry.entry_type.eq_ignore_ascii_case("table") {
            return Err(sqlite_master_corrupt(format!(
                "sqlite_master entry `{}` has unsupported type `{}`",
                entry.name, entry.entry_type
            )));
        }
        let Some(create_sql) = entry.sql else {
            return Err(sqlite_master_corrupt(format!(
                "sqlite_master table `{}` has NULL sql",
                entry.name
            )));
        };
        if !entry.name.eq_ignore_ascii_case(entry.table_name) {
            return Err(sqlite_master_corrupt(format!(
                "sqlite_master table `{}` has mismatched tbl_name `{}`",
                entry.name, entry.table_name
            )));
        }

        if is_virtual_table_sql(create_sql) {
            if entry.root_page < 0 {
                return Err(sqlite_master_corrupt(format!(
                    "sqlite_master virtual table `{}` has invalid rootpage {}",
                    entry.name, entry.root_page
                )));
            }
            let Some(Statement::CreateVirtualTable(create)) = parse_single_statement(create_sql)
            else {
                return Err(sqlite_master_corrupt(format!(
                    "could not parse CREATE VIRTUAL TABLE SQL for `{}`",
                    entry.name
                )));
            };
            if !create.name.name.eq_ignore_ascii_case(entry.name) {
                return Err(sqlite_master_corrupt(format!(
                    "CREATE VIRTUAL TABLE SQL for `{}` declares `{}`",
                    entry.name, create.name.name
                )));
            }
            if !qualified_catalog_name_targets_main(create.name.schema.as_deref()) {
                return Err(sqlite_master_corrupt(format!(
                    "CREATE VIRTUAL TABLE SQL for `{}` targets a non-main schema",
                    entry.name
                )));
            }
            if entry.root_page > 0 {
                claim_sqlite_master_root(
                    "virtual table",
                    entry.name,
                    entry.root_page,
                    max_root_page,
                    header,
                    free_pages,
                    &mut root_owners,
                )?;
            }
            claim_sqlite_master_virtual_table(
                row_index,
                entry.name,
                entry.root_page,
                &create.module,
                &create.args,
                &mut schema_names,
                &mut virtual_table_variants,
            )?;
            continue;
        }

        claim_sqlite_master_root(
            "table",
            entry.name,
            entry.root_page,
            max_root_page,
            header,
            free_pages,
            &mut root_owners,
        )?;
        let Some(Statement::CreateTable(create)) = parse_single_statement(create_sql) else {
            return Err(sqlite_master_corrupt(format!(
                "could not parse CREATE TABLE SQL for `{}`",
                entry.name
            )));
        };
        if create.temporary
            || !qualified_catalog_name_targets_main(create.name.schema.as_deref())
            || !create.name.name.eq_ignore_ascii_case(entry.name)
            || !create.name.name.eq_ignore_ascii_case(entry.table_name)
        {
            return Err(sqlite_master_corrupt(format!(
                "CREATE TABLE SQL for `{}` declares a temporary, non-main, or differently named table `{}`",
                entry.name, create.name.name
            )));
        }
        let slots = match &create.body {
            CreateTableBody::Columns {
                columns,
                constraints,
            } => implicit_autoindex_layout(columns, constraints, create.without_rowid).map_err(
                |error| {
                    sqlite_master_corrupt(format!(
                        "invalid implicit autoindex layout for table `{}`: {error}",
                        entry.name
                    ))
                },
            )?,
            CreateTableBody::AsSelect(_) => {
                return Err(sqlite_master_corrupt(format!(
                    "sqlite_master table `{}` stores CREATE TABLE AS SELECT instead of a normalized column definition",
                    entry.name
                )));
            }
        };
        let slot_count = slots.len();
        for ordinal in 1..=slot_count {
            let logical_name = format!("sqlite_autoindex_{}_{ordinal}", entry.name);
            logical_autoindex_names.insert(logical_name.to_ascii_lowercase(), logical_name);
        }
        let key = entry.name.to_ascii_lowercase();
        let pending = PendingTableAutoindexes {
            table_name: entry.name.to_owned(),
            slots,
            physical_roots: vec![None; slot_count],
        };
        if let Some(existing) = pending_by_table.insert(key, pending) {
            return Err(sqlite_master_corrupt(format!(
                "sqlite_master contains duplicate table entries for `{}` and `{}`",
                existing.table_name, entry.name
            )));
        }
        claim_sqlite_master_schema_name(
            entry.name,
            SqliteMasterSchemaNameKind::Table,
            &mut schema_names,
        )?;
    }

    for (trigger_name, table_name, requires_view) in pending_trigger_parents {
        let target = schema_names.get(&table_name.to_ascii_lowercase());
        let target_is_view =
            target.is_some_and(|owner| owner.kind == SqliteMasterSchemaNameKind::View);
        let target_is_table = target.is_some_and(|owner| {
            matches!(
                owner.kind,
                SqliteMasterSchemaNameKind::Table | SqliteMasterSchemaNameKind::VirtualTable
            )
        });
        if (!requires_view && !target_is_table) || (requires_view && !target_is_view) {
            let expected_kind = if requires_view { "view" } else { "table" };
            return Err(sqlite_master_corrupt(format!(
                "trigger `{trigger_name}` refers to missing or incompatible {expected_kind} `{table_name}`"
            )));
        }
    }

    let mut index_names = HashMap::<String, String>::new();
    for entry in &decoded {
        if !entry.entry_type.eq_ignore_ascii_case("index") {
            continue;
        }
        let index_key = entry.name.to_ascii_lowercase();
        let logical_autoindex_name = logical_autoindex_names.get(&index_key);
        if let Some(existing_name) = index_names.insert(index_key, entry.name.to_owned()) {
            return Err(sqlite_master_corrupt(format!(
                "sqlite_master contains duplicate index entries for `{existing_name}` and `{}`",
                entry.name
            )));
        }
        claim_sqlite_master_schema_name(
            entry.name,
            SqliteMasterSchemaNameKind::Index,
            &mut schema_names,
        )?;
        let root_page = claim_sqlite_master_root(
            "index",
            entry.name,
            entry.root_page,
            max_root_page,
            header,
            free_pages,
            &mut root_owners,
        )?;
        let table_key = entry.table_name.to_ascii_lowercase();
        if !pending_by_table.contains_key(&table_key) {
            return Err(sqlite_master_corrupt(format!(
                "index `{}` refers to missing ordinary table `{}`",
                entry.name, entry.table_name
            )));
        }
        // A stored CREATE INDEX statement is authoritative even when its name
        // resembles SQLite's reserved autoindex naming convention, unless a
        // real declaration slot (including a hidden WITHOUT ROWID PK slot)
        // already owns that logical name.
        if let Some(create_sql) = entry.sql {
            if let Some(logical_name) = logical_autoindex_name {
                return Err(sqlite_master_corrupt(format!(
                    "explicit index `{}` collides with logical implicit index `{logical_name}`",
                    entry.name
                )));
            }
            let Some(Statement::CreateIndex(create)) = parse_single_statement(create_sql) else {
                return Err(sqlite_master_corrupt(format!(
                    "could not parse CREATE INDEX SQL for `{}`",
                    entry.name
                )));
            };
            if !create.name.name.eq_ignore_ascii_case(entry.name)
                || !create.table.eq_ignore_ascii_case(entry.table_name)
                || !qualified_catalog_name_targets_main(create.name.schema.as_deref())
            {
                return Err(sqlite_master_corrupt(format!(
                    "CREATE INDEX SQL for `{}` declares index `{}` on table `{}` instead of `{}`",
                    entry.name, create.name.name, create.table, entry.table_name
                )));
            }
            continue;
        }

        let Some(table) = pending_by_table.get_mut(&table_key) else {
            unreachable!("ordinary index parent was validated above");
        };
        let Some(ordinal) = parse_autoindex_ordinal(entry.name, entry.table_name) else {
            return Err(sqlite_master_corrupt(format!(
                "implicit index `{}` does not have a canonical autoindex name for table `{}`",
                entry.name, entry.table_name
            )));
        };
        let Some(slot_index) = ordinal.checked_sub(1) else {
            return Err(sqlite_master_corrupt(format!(
                "implicit index `{}` has invalid ordinal {ordinal}",
                entry.name
            )));
        };
        let Some(slot) = table.slots.get(slot_index) else {
            return Err(sqlite_master_corrupt(format!(
                "implicit index `{}` selects nonexistent declaration slot {ordinal} on table `{}`",
                entry.name, table.table_name
            )));
        };
        if slot.is_hidden_without_rowid_primary_key() {
            return Err(sqlite_master_corrupt(format!(
                "implicit index `{}` illegally materializes hidden WITHOUT ROWID primary-key slot {ordinal}",
                entry.name
            )));
        }
        let root = table
            .physical_roots
            .get_mut(slot_index)
            .expect("validated implicit autoindex slot must have a root binding");
        if root.replace(root_page).is_some() {
            return Err(sqlite_master_corrupt(format!(
                "implicit autoindex slot {ordinal} on table `{}` is bound more than once",
                table.table_name
            )));
        }
    }

    let mut by_table = HashMap::with_capacity(pending_by_table.len());
    for (table_key, pending) in pending_by_table {
        let mut bound_slots = Vec::with_capacity(pending.slots.len());
        for (slot_index, slot) in pending.slots.into_iter().enumerate() {
            let ordinal = slot_index + 1;
            let storage = if slot.is_hidden_without_rowid_primary_key() {
                BoundImplicitAutoindexStorage::TableRoot
            } else {
                let root_page = pending.physical_roots[slot_index].ok_or_else(|| {
                    sqlite_master_corrupt(format!(
                        "sqlite_master is missing implicit autoindex slot {ordinal} for table `{}`",
                        pending.table_name
                    ))
                })?;
                BoundImplicitAutoindexStorage::IndexRoot(root_page)
            };
            bound_slots.push(BoundImplicitAutoindexSlot {
                ordinal,
                slot,
                storage,
            });
        }
        by_table.insert(table_key, BoundTableAutoindexes { slots: bound_slots });
    }

    let canonical_virtual_table_rows = virtual_table_variants
        .values()
        .map(|variants| canonical_virtual_table_variant(variants).row_index)
        .collect();

    Ok(BoundImplicitAutoindexCatalog {
        by_table,
        canonical_virtual_table_rows,
    })
}

#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
fn load_sqlite_header_from_page1(page1_bytes: &[u8]) -> Result<DatabaseHeader> {
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
    DatabaseHeader::from_bytes(header_bytes).map_err(|error| FrankenError::DatabaseCorrupt {
        detail: format!("invalid database header: {error}"),
    })
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

        // Allocate a fresh root page for this table in the on-disk file.
        let root_page = txn.allocate_page(cx).await?;

        // Initialize the root page as an empty leaf table B-tree.
        init_leaf_table_page(cx, &mut txn, root_page, page_size_usize, usable_size).await?;

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
            let idx_root = txn.allocate_page(cx).await?;
            init_leaf_index_page(cx, &mut txn, idx_root, page_size_usize, usable_size).await?;

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
                // GH #304: carry the declared key semantics into the physical
                // builder. `key_sort_directions` / `key_collations` were
                // previously consumed only when regenerating CREATE INDEX text,
                // so a DESC or non-BINARY term produced a b-tree whose physical
                // order contradicted its own sqlite_master declaration. Stock
                // SQLite then reads the index with the declared semantics and
                // reports the image as malformed.
                //
                // Scope of the trailing entry, stated exactly: the key loop
                // below emits a *synthetic* integer rowid as the suffix
                // (`key_values.push(SqliteValue::Integer(rowid))`). That layout
                // is correct only for rowid tables. A WITHOUT ROWID index takes
                // its primary-key columns as the suffix instead, and this
                // builder does not produce that shape at all — so the single
                // trailing ASC/BINARY entry pushed here describes the synthetic
                // rowid this code actually writes, and must NOT be read as
                // implementing SQLite's general index-suffix rule. WITHOUT
                // ROWID suffix semantics remain unfixed (GH #304 acceptance).
                //
                // Collations resolve against the cursor's default registry
                // (BINARY/NOCASE/RTRIM only). A collation registered on the
                // source connection is still not reachable from this function,
                // but it no longer falls back silently: an unresolvable *name* is
                // refused below, before any row is inserted, so a mis-ordered
                // image is never produced for it.
                //
                // What remains unresolved for GH #304 is narrower and is a
                // name-collision problem rather than a missing-name one: a source
                // connection that overrides a built-in — registering its own
                // implementation under BINARY, NOCASE, or RTRIM — still passes
                // the name check and is then built with the default
                // implementation. See the guard below for the full statement.
                //
                // Derive arity from the SAME discriminator the key loop below
                // uses, not from `key_term_count()`: that helper prefers
                // `key_expressions` whenever it is non-empty, while the loop
                // only takes the expression branch when `columns` is empty.
                // If both were ever populated the two would disagree and the
                // metadata vectors would silently mis-align with the key.
                let key_terms = if is_expression_index {
                    index.key_expressions.len()
                } else {
                    index.columns.len()
                };
                let mut index_desc_flags: Vec<bool> = (0..key_terms)
                    .map(|term| {
                        index.key_sort_directions.get(term).copied() == Some(SortDirection::Desc)
                    })
                    .collect();
                index_desc_flags.push(false);
                let mut index_collations: Vec<Option<String>> = (0..key_terms)
                    .map(|term| index.key_collations.get(term).cloned().flatten())
                    .collect();
                index_collations.push(None);

                let mut idx_cursor = fsqlite_btree::BtCursor::new_with_index_desc(
                    TransactionPageIo::new(&mut txn),
                    idx_root,
                    usable_size,
                    true,
                    index_desc_flags,
                );
                let collation_registry = idx_cursor.collation_registry();
                // GH #304: a collation the builder cannot resolve silently
                // degrades to BINARY inside the cursor comparator, producing an
                // index whose physical order contradicts the `COLLATE` term in
                // its own regenerated DDL — exactly the malformed-image class
                // this issue was filed for, but without the DESC symptom that
                // made the original report visible. The source connection's
                // registry is not reachable from this function, so the honest
                // outcome is refusal rather than a quietly mis-ordered rebuild.
                // This mirrors the hidden WITHOUT ROWID primary-key slot, which
                // is likewise refused instead of approximated.
                //
                // This is a supported-schema limitation, not a violated internal
                // invariant, so it is reported as `NotImplemented`: the schema is
                // legitimate and the caller can act on it (register the collation
                // on the rebuilding path, or export without that index).
                //
                // KNOWN GAP (GH #304, unresolved): this guard keys on the
                // presence of a *name*, so it cannot see a source connection that
                // overrides a built-in — registering its own implementation under
                // `BINARY`, `NOCASE`, or `RTRIM`. `contains()` answers true, the
                // guard admits the index, and the builder then orders it with the
                // default implementation instead of the caller's. That is the
                // same silently-wrong-order defect this guard exists to prevent,
                // reachable through a name the guard trusts. Closing it needs the
                // source connection's registry (or a per-collation identity, not
                // just a name), which is not reachable from this function.
                //
                // Cleanup of the partially written candidate is NOT performed
                // here — this function never removes its own output on any error
                // path. The enclosing VACUUM caller owns that through
                // `VacuumTargetReservation`, which is identity-bound; returning
                // early simply leaves the reservation to clean up as it already
                // does for every other failure in this function.
                {
                    let registry = collation_registry.lock().map_err(|_| {
                        FrankenError::internal(
                            "collation registry lock poisoned while rebuilding an index".to_owned(),
                        )
                    })?;
                    for collation in index_collations.iter().flatten() {
                        if !registry.contains(collation) {
                            return Err(FrankenError::not_implemented(format!(
                                "index `{}` declares collation `{collation}`, which is not \
                                 available to the compatibility index builder; rebuilding it \
                                 would order the index by BINARY and contradict its own \
                                 declaration",
                                index.name
                            )));
                        }
                    }
                }
                idx_cursor.set_index_collation_context(index_collations, collation_registry);
                configure_btree_cursor_page_size(&mut idx_cursor, usable_size, full_page_size);
                if let Some(mem_table) = db.get_table(table.root_page) {
                    for (rowid, values) in mem_table.iter_rows() {
                        // For partial indexes, skip rows that don't match
                        // the WHERE predicate. If evaluation fails, include
                        // the row (safe default).
                        if let Some(ref predicate) = partial_predicate
                            && let Ok(result) = eval_join_expr(predicate, values, &col_map)
                            && !is_sqlite_truthy(&result)
                        {
                            continue;
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
                        idx_cursor.index_insert(cx, &key).await?;
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
    let max_root_page = txn.snapshot_db_size();
    let page1 = txn.get_page(cx, PageNumber::ONE).await?;
    let header = load_sqlite_header_from_page1(page1.as_ref())?;
    let usable_size = header.page_size.usable(header.reserved_per_page);
    let page_size = header.page_size.get();

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

    let free_pages = txn.live_freelist_pages().into_iter().collect();
    let bound_implicit_autoindexes =
        bind_implicit_autoindex_catalog(&master_entries, max_root_page, &header, &free_pages)?;

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
        if !entry_type.eq_ignore_ascii_case("table") {
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
        let is_virtual_table = is_virtual_table_sql(&create_sql);
        if root_page_num == 0 && is_virtual_table {
            let _shadowed_by_materialized =
                materialized_virtual_tables.contains(&name.to_ascii_lowercase());
            continue;
        }
        let root_page_u32 = validate_sqlite_master_root_page(&name, root_page_num)?;

        // Parse the CREATE TABLE to extract column info and schema decorations.
        let columns = parse_columns_from_sqlite_master_sql(&create_sql);
        let bound_table_autoindexes = if is_virtual_table {
            None
        } else {
            Some(bound_implicit_autoindexes.table(&name).ok_or_else(|| {
                sqlite_master_corrupt(format!(
                    "validated ordinary table `{name}` has no bound autoindex layout"
                ))
            })?)
        };
        let indexes = bound_table_autoindexes
            .map_or_else(Vec::new, |bound| bound.physical_index_schemas(&name));
        let primary_key_constraints = extract_primary_key_constraints_from_sql(&create_sql);
        let foreign_keys = extract_foreign_keys_from_sql(&create_sql, &columns);
        let check_constraints = extract_check_constraints_with_owners_from_sql(&create_sql);
        let num_columns = columns.len();
        let without_rowid = is_without_rowid_table_sql(&create_sql);
        let ipk_col_idx = columns.iter().position(|c| c.is_ipk);

        // Use the REAL root page from sqlite_master (5A.4: bd-1soh).
        let real_root_page =
            i32::try_from(root_page_u32).expect("validated root page must fit MemDatabase");
        db.create_table_at(real_root_page, num_columns);
        for index in &indexes {
            db.create_table_at(index.root_page, 0);
        }

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
            for slot in bound_table_autoindexes
                .into_iter()
                .flat_map(BoundTableAutoindexes::implicit_slots)
            {
                let Some(column_indices) = slot
                    .columns()
                    .iter()
                    .map(|column_name| {
                        current_table_schema
                            .columns
                            .iter()
                            .position(|column| column.name.eq_ignore_ascii_case(column_name))
                    })
                    .collect::<Option<Vec<_>>>()
                else {
                    return Err(FrankenError::DatabaseCorrupt {
                        detail: format!(
                            "canonical autoindex layout for `{table_name_for_err}` references a missing column"
                        ),
                    });
                };
                if !column_indices.is_empty() {
                    mem_table.add_unique_column_group_with_collations(
                        column_indices,
                        slot.key_collations().to_vec(),
                    );
                }
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
                        inflate_loaded_table_row_values(
                            &mut values,
                            synthetic_rowid,
                            &current_table_schema.columns,
                            None,
                            &table_name_for_err,
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

        // Find the parent table in the schema and bind the authoritative SQL
        // against it before mutating either schema or MemDatabase state.
        let Some(table_position) = schema
            .iter()
            .position(|table| table.name.eq_ignore_ascii_case(&tbl_name))
        else {
            return Err(sqlite_master_corrupt(format!(
                "validated index `{index_name}` lost parent table `{tbl_name}` during load"
            )));
        };

        let Some(Statement::CreateIndex(create_stmt)) = parse_single_statement(&create_sql) else {
            return Err(sqlite_master_corrupt(format!(
                "validated CREATE INDEX SQL for `{index_name}` could not be parsed during load"
            )));
        };
        if let Some(schema_name) = create_stmt.name.schema.as_deref() {
            return Err(sqlite_master_corrupt(format!(
                "explicit index `{index_name}` on table `{tbl_name}` has non-canonical schema-qualified CREATE INDEX SQL (`{schema_name}`) during load"
            )));
        }
        let table = &schema[table_position];
        if table
            .indexes
            .iter()
            .any(|index| index.name.eq_ignore_ascii_case(&index_name))
        {
            return Err(sqlite_master_corrupt(format!(
                "validated explicit index `{index_name}` duplicates an existing index on table `{tbl_name}` during load"
            )));
        }
        let bound_index = bind_explicit_index(&create_stmt, &index_name, &tbl_name, table)
            .map_err(|error| {
                sqlite_master_corrupt(format!(
                    "invalid explicit index `{index_name}` on table `{tbl_name}` during load: {error}"
                ))
            })?;
        for indexed in &create_stmt.columns {
            validate_builtin_persisted_index_expr_functions(&indexed.expr).map_err(|error| {
                sqlite_master_corrupt(format!(
                    "invalid explicit index `{index_name}` on table `{tbl_name}` during load: {error}"
                ))
            })?;
        }
        if let Some(predicate) = create_stmt.where_clause.as_ref() {
            validate_builtin_persisted_index_expr_functions(predicate).map_err(|error| {
                sqlite_master_corrupt(format!(
                    "invalid explicit index `{index_name}` on table `{tbl_name}` during load: {error}"
                ))
            })?;
        }
        schema[table_position]
            .indexes
            .push(bound_index.into_index_schema(root_page_i32));
        db.create_table_at(root_page_i32, 0);
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

#[cfg(test)]
fn extract_unique_constraint_indexes_from_sql(
    sql: &str,
    table_name: &str,
) -> Result<Vec<IndexSchema>> {
    let slots = extract_implicit_autoindex_slots_from_sql(sql, table_name)?;
    Ok(slots
        .into_iter()
        .enumerate()
        .filter(|(_, slot)| !slot.is_hidden_without_rowid_primary_key())
        .map(|(slot_index, slot)| slot.into_index_schema(table_name, slot_index + 1, 0))
        .collect())
}

#[cfg(test)]
fn extract_implicit_autoindex_slots_from_sql(
    sql: &str,
    table_name: &str,
) -> Result<Vec<ImplicitAutoindexSlot>> {
    let Some(Statement::CreateTable(create)) = parse_single_statement(sql) else {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!("could not parse CREATE TABLE SQL for `{table_name}`"),
        });
    };
    if !create.name.name.eq_ignore_ascii_case(table_name) {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "CREATE TABLE SQL for `{table_name}` declares `{}`",
                create.name.name
            ),
        });
    }
    let CreateTableBody::Columns {
        columns,
        constraints,
    } = &create.body
    else {
        return Ok(Vec::new());
    };
    implicit_autoindex_layout(columns, constraints, create.without_rowid)
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
        if let TableConstraintKind::Check(expr) = &constraint.kind {
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

fn indexed_column_name(indexed_column: &fsqlite_ast::IndexedColumn) -> Option<&str> {
    fn extract(expr: &Expr) -> Option<&str> {
        match expr {
            Expr::Column(column, _) if column.table.is_none() => Some(&column.column),
            // SQLite accepts a legacy single-quoted identifier in table-level
            // PRIMARY KEY and UNIQUE constraints.
            Expr::Literal(Literal::String(name), _) => Some(name),
            Expr::Collate { expr, .. } => extract(expr),
            _ => None,
        }
    }

    extract(&indexed_column.expr)
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

    let mut table_pk_rowid = None;

    if let CreateTableBody::Columns { constraints, .. } = &create.body {
        for constraint in constraints {
            match &constraint.kind {
                TableConstraintKind::PrimaryKey {
                    columns: pk_columns,
                    conflict,
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

                    let is_integer = column_def_is_exact_integer(&columns[index]);
                    if is_integer && !create.without_rowid {
                        table_pk_rowid = Some((index, *conflict));
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
            let is_integer = column_def_is_exact_integer(col);
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
        .or_else(|| table_pk_rowid.map(|(index, _)| index));

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
                let collation = col.constraints.iter().rev().find_map(|constraint| {
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
                        .or_else(|| {
                            table_pk_rowid.and_then(|(pk_index, conflict)| {
                                (pk_index == index).then_some(conflict).flatten()
                            })
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

    fn bare_table_schema(name: &str, columns: &[&str]) -> TableSchema {
        TableSchema {
            name: name.to_owned(),
            root_page: 2,
            columns: columns
                .iter()
                .map(|column| ColumnInfo::basic(*column, 'A', false))
                .collect(),
            indexes: Vec::new(),
            strict: false,
            without_rowid: false,
            primary_key_constraints: Vec::new(),
            foreign_keys: Vec::new(),
            check_constraints: Vec::new(),
        }
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
    fn test_parse_columns_from_create_sql_legacy_quoted_integer_primary_key_is_ipk() {
        let sql = "CREATE TABLE metrics (id INTEGER, body TEXT, PRIMARY KEY('id'))";
        let cols = parse_columns_from_create_sql(sql);
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].name, "id");
        assert!(cols[0].is_ipk);
        assert_eq!(cols[1].name, "body");
        assert_eq!(
            extract_primary_key_constraints_from_sql(sql),
            vec![vec!["id".to_owned()]]
        );
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

    #[test]
    fn test_extract_implicit_autoindexes_preserves_without_rowid_slots_and_exact_integer_rules() {
        let indexes = extract_unique_constraint_indexes_from_sql(
            "CREATE TABLE wr(
                pk TEXT PRIMARY KEY,
                u TEXT COLLATE NOCASE COLLATE RTRIM UNIQUE
             ) WITHOUT ROWID",
            "wr",
        )
        .unwrap();
        assert_eq!(indexes.len(), 1);
        assert_eq!(indexes[0].name, "sqlite_autoindex_wr_2");
        assert_eq!(indexes[0].columns, ["u"]);
        assert_eq!(indexes[0].key_collations, [Some("RTRIM".to_owned())]);

        let typed = extract_unique_constraint_indexes_from_sql(
            "CREATE TABLE typed(id INTEGER(8) PRIMARY KEY, u TEXT UNIQUE)",
            "typed",
        )
        .unwrap();
        assert_eq!(
            typed
                .iter()
                .map(|index| index.name.as_str())
                .collect::<Vec<_>>(),
            ["sqlite_autoindex_typed_1", "sqlite_autoindex_typed_2"]
        );
    }

    #[test]
    fn test_autoindex_followup_explicit_index_uses_final_repeated_collation() {
        let Some(Statement::CreateIndex(create)) =
            parse_single_statement("CREATE INDEX idx_t_a ON t(a COLLATE NOCASE COLLATE RTRIM)")
        else {
            panic!("expected CREATE INDEX");
        };
        let table = bare_table_schema("t", &["a"]);
        let index = bind_explicit_index(&create, "idx_t_a", "t", &table)
            .expect("authoritative index binder should accept repeated COLLATE syntax")
            .into_index_schema(7);
        assert_eq!(index.columns, ["a"]);
        assert_eq!(index.key_collations, [Some("RTRIM".to_owned())]);
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
    fn test_check_constraint_fallback_preserves_column_ownership() {
        // SQLite accepts a conflict clause after a table CHECK, while the AST
        // parser currently rejects that suffix. Exercise the fallback so a
        // neighboring column CHECK does not get flattened into table scope.
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
    fn test_foreign_key_fallback_preserves_ownership_and_actions() {
        // The trailing CHECK conflict clause is accepted by SQLite but is not
        // yet accepted by the full AST parser, forcing the schema fallback.
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

        let Some(Statement::CreateIndex(create)) = parse_single_statement(sql) else {
            panic!("expected CREATE INDEX");
        };
        let table = bare_table_schema(
            "items(table)",
            &["last, name", "code", "tag", "ord", "active"],
        );
        let idx = bind_explicit_index(&create, "idx(words)", "items(table)", &table)
            .expect("authoritative index binder should preserve quoted metadata")
            .into_index_schema(7);

        assert_eq!(
            idx.columns,
            vec![
                "last, name".to_owned(),
                "code".to_owned(),
                "tag".to_owned(),
                "ord".to_owned()
            ]
        );
        // Collation names are compared case-insensitively by SQLite
        // (`sqlite3_strnicmp`), so `key_collations` carries semantic schema
        // state rather than source spelling. Assert the identity of all four
        // names — including that `COLLATE DESC` binds a collation *named*
        // `DESC` rather than being absorbed as a sort direction — without
        // pinning the case a binder happens to emit. The length and `Some`
        // checks stay explicit so a dropped or `None` term still fails.
        let expected_collations = ["RTRIM", "BINARY", "DESC", "DESC"];
        assert_eq!(
            idx.key_collations.len(),
            expected_collations.len(),
            "every key term must bind a collation: {:?}",
            idx.key_collations
        );
        for (term, expected) in expected_collations.iter().enumerate() {
            let actual = idx.key_collations[term].as_deref().unwrap_or_else(|| {
                panic!("term {term} must bind collation `{expected}`, found None")
            });
            assert!(
                actual.eq_ignore_ascii_case(expected),
                "term {term} must bind collation `{expected}` (case-insensitively), found `{actual}`"
            );
        }
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

        let Some(Statement::CreateIndex(create)) = parse_single_statement(sql) else {
            panic!("expected CREATE INDEX");
        };
        let table = bare_table_schema("agents", &["name", "is_active"]);
        let idx = bind_explicit_index(&create, "uq_agents_name_ci", "agents", &table)
            .expect("authoritative index binder should preserve expression metadata")
            .into_index_schema(7);

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

    fn implicit_autoindex_catalog_row(
        entry_type: &str,
        name: &str,
        table_name: &str,
        root_page: i64,
        sql: Option<&str>,
    ) -> Vec<SqliteValue> {
        vec![
            SqliteValue::Text(entry_type.into()),
            SqliteValue::Text(name.into()),
            SqliteValue::Text(table_name.into()),
            SqliteValue::Integer(root_page),
            sql.map_or(SqliteValue::Null, |value| SqliteValue::Text(value.into())),
        ]
    }

    fn assert_implicit_autoindex_catalog_corrupt(
        case_name: &str,
        entries: &[Vec<SqliteValue>],
        detail_needle: &str,
    ) {
        assert_implicit_autoindex_catalog_corrupt_with_page_bound(
            case_name,
            entries,
            i32::MAX.unsigned_abs(),
            detail_needle,
        );
    }

    fn assert_implicit_autoindex_catalog_corrupt_with_page_bound(
        case_name: &str,
        entries: &[Vec<SqliteValue>],
        max_root_page: u32,
        detail_needle: &str,
    ) {
        let header = DatabaseHeader::default();
        assert_implicit_autoindex_catalog_corrupt_with_root_context(
            case_name,
            entries,
            max_root_page,
            &header,
            &HashSet::new(),
            detail_needle,
        );
    }

    fn assert_implicit_autoindex_catalog_corrupt_with_root_context(
        case_name: &str,
        entries: &[Vec<SqliteValue>],
        max_root_page: u32,
        header: &DatabaseHeader,
        free_pages: &HashSet<PageNumber>,
        detail_needle: &str,
    ) {
        let error = bind_implicit_autoindex_catalog(entries, max_root_page, header, free_pages)
            .unwrap_err();
        let FrankenError::DatabaseCorrupt { detail } = error else {
            panic!("{case_name}: expected DatabaseCorrupt, found {error:?}");
        };
        assert!(
            detail.contains(detail_needle),
            "{case_name}: expected `{detail_needle}` in corruption detail, found `{detail}`"
        );
    }

    #[test]
    fn virtual_table_catalog_canonical_row_is_order_independent() {
        let cases = [
            (
                "contentless before stale default",
                vec![
                    implicit_autoindex_catalog_row(
                        "table",
                        "vt",
                        "vt",
                        0,
                        Some("CREATE VIRTUAL TABLE vt USING fts5(body, content='')"),
                    ),
                    implicit_autoindex_catalog_row(
                        "table",
                        "vt",
                        "vt",
                        0,
                        Some("CREATE VIRTUAL TABLE vt USING fts5(body)"),
                    ),
                ],
                0,
            ),
            (
                "contentless after stale default",
                vec![
                    implicit_autoindex_catalog_row(
                        "table",
                        "vt",
                        "vt",
                        0,
                        Some("CREATE VIRTUAL TABLE vt USING fts5(body)"),
                    ),
                    implicit_autoindex_catalog_row(
                        "table",
                        "vt",
                        "vt",
                        0,
                        Some("CREATE VIRTUAL TABLE vt USING fts5(body, content='')"),
                    ),
                ],
                1,
            ),
            (
                "equivalent root-zero rows",
                vec![
                    implicit_autoindex_catalog_row(
                        "table",
                        "vt",
                        "vt",
                        0,
                        Some("CREATE VIRTUAL TABLE vt USING fts5(body)"),
                    ),
                    implicit_autoindex_catalog_row(
                        "table",
                        "VT",
                        "VT",
                        0,
                        Some("CREATE VIRTUAL TABLE vt USING fts5(body)"),
                    ),
                ],
                0,
            ),
            (
                "positive row before migration row",
                vec![
                    implicit_autoindex_catalog_row(
                        "table",
                        "vt",
                        "vt",
                        2,
                        Some("CREATE VIRTUAL TABLE vt USING fts5(body)"),
                    ),
                    implicit_autoindex_catalog_row(
                        "table",
                        "vt",
                        "vt",
                        0,
                        Some("CREATE VIRTUAL TABLE vt USING fts5(body)"),
                    ),
                ],
                0,
            ),
            (
                "positive row after migration row",
                vec![
                    implicit_autoindex_catalog_row(
                        "table",
                        "vt",
                        "vt",
                        0,
                        Some("CREATE VIRTUAL TABLE vt USING fts5(body)"),
                    ),
                    implicit_autoindex_catalog_row(
                        "table",
                        "vt",
                        "vt",
                        2,
                        Some("CREATE VIRTUAL TABLE vt USING fts5(body)"),
                    ),
                ],
                1,
            ),
        ];

        for (case_name, entries, expected_row) in cases {
            let catalog = bind_implicit_autoindex_catalog(
                &entries,
                2,
                &DatabaseHeader::default(),
                &HashSet::new(),
            )
            .unwrap_or_else(|error| panic!("{case_name}: unexpected bind failure: {error}"));
            for row_index in 0..entries.len() {
                assert_eq!(
                    catalog.is_canonical_virtual_table_row(row_index),
                    row_index == expected_row,
                    "{case_name}: wrong canonical status for row {row_index}"
                );
            }
        }
    }

    #[test]
    fn implicit_autoindex_catalog_binds_complete_layout_and_real_roots() {
        let entries = vec![
            implicit_autoindex_catalog_row(
                "table",
                "t",
                "t",
                2,
                Some("CREATE TABLE t(a TEXT UNIQUE, b TEXT UNIQUE)"),
            ),
            // Deliberately reverse the physical rows: declaration ordinals,
            // not sqlite_master scan order, determine the canonical result.
            implicit_autoindex_catalog_row("index", "sqlite_autoindex_t_2", "t", 4, None),
            implicit_autoindex_catalog_row("index", "sqlite_autoindex_t_1", "t", 3, None),
            implicit_autoindex_catalog_row(
                "table",
                "wr",
                "wr",
                5,
                Some("CREATE TABLE wr(pk TEXT PRIMARY KEY, u TEXT UNIQUE) WITHOUT ROWID"),
            ),
            implicit_autoindex_catalog_row("index", "sqlite_autoindex_wr_2", "wr", 6, None),
            implicit_autoindex_catalog_row(
                "table",
                "plain",
                "plain",
                7,
                Some("CREATE TABLE plain(x TEXT)"),
            ),
            implicit_autoindex_catalog_row(
                "index",
                "sqlite_autoindex_plain_1",
                "plain",
                8,
                Some("CREATE INDEX sqlite_autoindex_plain_1 ON plain(x)"),
            ),
        ];

        let catalog = bind_implicit_autoindex_catalog(
            &entries,
            8,
            &DatabaseHeader::default(),
            &HashSet::new(),
        )
        .unwrap();
        let ordinary = catalog.table("T").unwrap();
        let physical = ordinary.physical_index_schemas("t");
        assert_eq!(
            physical
                .iter()
                .map(|index| (index.name.as_str(), index.root_page))
                .collect::<Vec<_>>(),
            vec![("sqlite_autoindex_t_1", 3), ("sqlite_autoindex_t_2", 4)]
        );

        let without_rowid = catalog.table("wr").unwrap();
        assert_eq!(
            without_rowid
                .slots
                .iter()
                .map(|bound| bound.storage)
                .collect::<Vec<_>>(),
            vec![
                BoundImplicitAutoindexStorage::TableRoot,
                BoundImplicitAutoindexStorage::IndexRoot(6)
            ]
        );
        let wr_physical = without_rowid.physical_index_schemas("wr");
        assert_eq!(wr_physical.len(), 1);
        assert_eq!(wr_physical[0].name, "sqlite_autoindex_wr_2");
        assert_eq!(wr_physical[0].root_page, 6);

        let plain = catalog.table("plain").unwrap();
        assert_eq!(plain.implicit_slots().count(), 0);
        assert!(plain.physical_index_schemas("plain").is_empty());
    }

    #[test]
    fn implicit_autoindex_catalog_accepts_valid_views_triggers_and_supported_fts5_repair() {
        let entries = vec![
            implicit_autoindex_catalog_row(
                "TaBlE",
                "plain",
                "plain",
                2,
                Some("CREATE TABLE main.plain(x TEXT)"),
            ),
            implicit_autoindex_catalog_row(
                "ViEw",
                "v",
                "v",
                0,
                Some("CREATE VIEW main.v AS SELECT x FROM plain"),
            ),
            implicit_autoindex_catalog_row(
                "TrIgGeR",
                "plain",
                "plain",
                0,
                Some("CREATE TRIGGER main.plain AFTER INSERT ON plain BEGIN SELECT 1; END"),
            ),
            implicit_autoindex_catalog_row(
                "trigger",
                "v",
                "v",
                0,
                Some("CREATE TRIGGER main.v INSTEAD OF INSERT ON v BEGIN SELECT 1; END"),
            ),
            implicit_autoindex_catalog_row(
                "table",
                "docs",
                "docs",
                0,
                Some("CREATE VIRTUAL TABLE main.docs USING fts5(title, body, content='')"),
            ),
            implicit_autoindex_catalog_row(
                "TABLE",
                "DOCS",
                "DOCS",
                0,
                Some("CREATE VIRTUAL TABLE docs USING fts5(title, body)"),
            ),
            implicit_autoindex_catalog_row(
                "table",
                "legacy_docs",
                "legacy_docs",
                3,
                Some("CREATE VIRTUAL TABLE legacy_docs USING fts5(title, body)"),
            ),
            implicit_autoindex_catalog_row(
                "table",
                "LEGACY_DOCS",
                "LEGACY_DOCS",
                0,
                Some("CREATE VIRTUAL TABLE legacy_docs USING fts5(title, body)"),
            ),
        ];

        let catalog = bind_implicit_autoindex_catalog(
            &entries,
            3,
            &DatabaseHeader::default(),
            &HashSet::new(),
        )
        .unwrap();
        assert!(catalog.table("PLAIN").is_some());
        assert!(catalog.table("docs").is_none());
    }

    #[test]
    fn implicit_autoindex_catalog_rejects_invalid_row_shapes_and_storage_classes() {
        let mut short_row =
            implicit_autoindex_catalog_row("table", "t", "t", 2, Some("CREATE TABLE t(a)"));
        short_row.pop();
        let mut non_text_type =
            implicit_autoindex_catalog_row("table", "t", "t", 2, Some("CREATE TABLE t(a)"));
        non_text_type[0] = SqliteValue::Integer(1);
        let mut non_text_name =
            implicit_autoindex_catalog_row("table", "t", "t", 2, Some("CREATE TABLE t(a)"));
        non_text_name[1] = SqliteValue::Null;
        let mut non_text_table_name =
            implicit_autoindex_catalog_row("table", "t", "t", 2, Some("CREATE TABLE t(a)"));
        non_text_table_name[2] = SqliteValue::Blob(vec![1].into());
        let mut invalid_sql_storage =
            implicit_autoindex_catalog_row("table", "t", "t", 2, Some("CREATE TABLE t(a)"));
        invalid_sql_storage[4] = SqliteValue::Integer(1);

        for (case_name, entries, detail_needle) in [
            ("short row", vec![short_row], "columns instead of 5"),
            (
                "non-text type",
                vec![non_text_type],
                "column `type` must be TEXT",
            ),
            (
                "non-text name",
                vec![non_text_name],
                "column `name` must be TEXT",
            ),
            (
                "non-text table name",
                vec![non_text_table_name],
                "column `tbl_name` must be TEXT",
            ),
            (
                "invalid sql storage",
                vec![invalid_sql_storage],
                "column `sql` must be TEXT or NULL",
            ),
            (
                "table NULL sql",
                vec![implicit_autoindex_catalog_row("table", "t", "t", 2, None)],
                "has NULL sql",
            ),
        ] {
            assert_implicit_autoindex_catalog_corrupt(case_name, &entries, detail_needle);
        }

        assert_implicit_autoindex_catalog_corrupt_with_page_bound(
            "zero visible bound",
            &[],
            0,
            "without a visible database page",
        );
    }

    #[test]
    fn implicit_autoindex_catalog_rejects_incomplete_ambiguous_or_unsafe_catalogs() {
        let unique_table = || {
            implicit_autoindex_catalog_row(
                "table",
                "t",
                "t",
                2,
                Some("CREATE TABLE t(a TEXT UNIQUE)"),
            )
        };
        let unique_index = |name: &str, root_page: i64| {
            implicit_autoindex_catalog_row("index", name, "t", root_page, None)
        };

        let mut non_integer_root = unique_table();
        non_integer_root[3] = SqliteValue::Text("two".into());
        let cases = vec![
            (
                "missing expected row",
                vec![unique_table()],
                "missing implicit autoindex",
            ),
            (
                "unexpected ordinal",
                vec![unique_table(), unique_index("sqlite_autoindex_t_2", 3)],
                "nonexistent declaration slot 2",
            ),
            (
                "duplicate row",
                vec![
                    unique_table(),
                    unique_index("sqlite_autoindex_t_1", 3),
                    unique_index("SQLITE_AUTOINDEX_T_1", 4),
                ],
                "duplicate index entries",
            ),
            (
                "hidden WITHOUT ROWID PK row",
                vec![
                    implicit_autoindex_catalog_row(
                        "table",
                        "wr",
                        "wr",
                        2,
                        Some("CREATE TABLE wr(pk TEXT PRIMARY KEY) WITHOUT ROWID"),
                    ),
                    implicit_autoindex_catalog_row("index", "sqlite_autoindex_wr_1", "wr", 3, None),
                ],
                "illegally materializes hidden",
            ),
            (
                "missing implicit parent",
                vec![implicit_autoindex_catalog_row(
                    "index",
                    "sqlite_autoindex_absent_1",
                    "absent",
                    3,
                    None,
                )],
                "missing ordinary table",
            ),
            (
                "missing explicit parent",
                vec![implicit_autoindex_catalog_row(
                    "index",
                    "idx_absent",
                    "absent",
                    3,
                    Some("CREATE INDEX idx_absent ON absent(a)"),
                )],
                "missing ordinary table",
            ),
            (
                "table name mismatch",
                vec![implicit_autoindex_catalog_row(
                    "table",
                    "t",
                    "other",
                    2,
                    Some("CREATE TABLE t(a)"),
                )],
                "mismatched tbl_name",
            ),
            (
                "CREATE TABLE name mismatch",
                vec![implicit_autoindex_catalog_row(
                    "table",
                    "t",
                    "t",
                    2,
                    Some("CREATE TABLE other(a)"),
                )],
                // The CREATE TABLE arm names the rejection class before the
                // offending name (compat_persist.rs:709), unlike the virtual
                // table arm below, which still renders `declares \`{}\``.
                "differently named table `other`",
            ),
            (
                "layout conflict mapping",
                vec![implicit_autoindex_catalog_row(
                    "table",
                    "t",
                    "t",
                    2,
                    Some(
                        "CREATE TABLE t(a TEXT UNIQUE ON CONFLICT IGNORE, UNIQUE(a) ON CONFLICT REPLACE)",
                    ),
                )],
                "invalid implicit autoindex layout",
            ),
            (
                "non-integer root",
                vec![non_integer_root],
                "must be INTEGER",
            ),
            (
                "zero root",
                vec![implicit_autoindex_catalog_row(
                    "table",
                    "t",
                    "t",
                    0,
                    Some("CREATE TABLE t(a)"),
                )],
                "invalid rootpage 0",
            ),
            (
                "negative root",
                vec![implicit_autoindex_catalog_row(
                    "table",
                    "t",
                    "t",
                    -2,
                    Some("CREATE TABLE t(a)"),
                )],
                "invalid rootpage -2",
            ),
            (
                "above i32 root",
                vec![implicit_autoindex_catalog_row(
                    "table",
                    "t",
                    "t",
                    i64::from(i32::MAX) + 1,
                    Some("CREATE TABLE t(a)"),
                )],
                "exceeds supported range",
            ),
            (
                "penultimate i32 root",
                vec![implicit_autoindex_catalog_row(
                    "table",
                    "t",
                    "t",
                    i64::from(i32::MAX - 1),
                    Some("CREATE TABLE t(a)"),
                )],
                "no safe MemDatabase allocation sentinel",
            ),
            (
                "terminal i32 root",
                vec![implicit_autoindex_catalog_row(
                    "table",
                    "t",
                    "t",
                    i64::from(i32::MAX),
                    Some("CREATE TABLE t(a)"),
                )],
                "no safe MemDatabase allocation sentinel",
            ),
            (
                "page one collision",
                vec![implicit_autoindex_catalog_row(
                    "table",
                    "t",
                    "t",
                    1,
                    Some("CREATE TABLE t(a)"),
                )],
                "shared by sqlite_master",
            ),
            (
                "table index root collision",
                vec![unique_table(), unique_index("sqlite_autoindex_t_1", 2)],
                "rootpage 2 is shared",
            ),
            (
                "two index root collision",
                vec![
                    implicit_autoindex_catalog_row(
                        "table",
                        "t",
                        "t",
                        2,
                        Some("CREATE TABLE t(a UNIQUE, b UNIQUE)"),
                    ),
                    unique_index("sqlite_autoindex_t_1", 3),
                    unique_index("sqlite_autoindex_t_2", 3),
                ],
                "rootpage 3 is shared",
            ),
            (
                "explicit index identity mismatch",
                vec![
                    implicit_autoindex_catalog_row("table", "t", "t", 2, Some("CREATE TABLE t(a)")),
                    implicit_autoindex_catalog_row(
                        "index",
                        "idx_t",
                        "t",
                        3,
                        Some("CREATE INDEX idx_other ON t(a)"),
                    ),
                ],
                "declares index `idx_other`",
            ),
            (
                "virtual table identity mismatch",
                vec![implicit_autoindex_catalog_row(
                    "table",
                    "vt",
                    "vt",
                    0,
                    Some("CREATE VIRTUAL TABLE other USING fts5(body)"),
                )],
                "declares `other`",
            ),
            (
                "unsupported schema type",
                vec![implicit_autoindex_catalog_row(
                    "bogus",
                    "x",
                    "x",
                    0,
                    Some("bogus"),
                )],
                "unsupported type",
            ),
            (
                "view owns a root",
                vec![implicit_autoindex_catalog_row(
                    "view",
                    "v",
                    "v",
                    2,
                    Some("CREATE VIEW v AS SELECT 1"),
                )],
                "must have rootpage 0",
            ),
        ];

        for (case_name, entries, detail_needle) in cases {
            assert_implicit_autoindex_catalog_corrupt(case_name, &entries, detail_needle);
        }

        assert_implicit_autoindex_catalog_corrupt_with_page_bound(
            "root past visible database",
            &[
                implicit_autoindex_catalog_row(
                    "table",
                    "t",
                    "t",
                    2,
                    Some("CREATE TABLE t(a TEXT UNIQUE)"),
                ),
                implicit_autoindex_catalog_row("index", "sqlite_autoindex_t_1", "t", 4, None),
            ],
            3,
            "exceeds the visible database page count 3",
        );
    }

    #[test]
    fn implicit_autoindex_catalog_rejects_schema_identity_and_namespace_corruption() {
        let ordinary_table =
            || implicit_autoindex_catalog_row("table", "t", "t", 2, Some("CREATE TABLE t(a TEXT)"));
        let cases = vec![
            (
                "ordinary CREATE parse failure",
                vec![implicit_autoindex_catalog_row(
                    "table",
                    "t",
                    "t",
                    2,
                    Some("CREATE TABLE t("),
                )],
                "could not parse CREATE TABLE",
            ),
            (
                "stored CTAS",
                vec![implicit_autoindex_catalog_row(
                    "table",
                    "t",
                    "t",
                    2,
                    Some("CREATE TABLE t AS SELECT 1 AS a"),
                )],
                "CREATE TABLE AS SELECT",
            ),
            (
                "duplicate ordinary table",
                vec![
                    ordinary_table(),
                    implicit_autoindex_catalog_row(
                        "table",
                        "T",
                        "T",
                        3,
                        Some("CREATE TABLE T(a TEXT)"),
                    ),
                ],
                "duplicate table entries",
            ),
            (
                "table view namespace collision",
                vec![
                    ordinary_table(),
                    implicit_autoindex_catalog_row(
                        "view",
                        "T",
                        "T",
                        0,
                        Some("CREATE VIEW T AS SELECT 1"),
                    ),
                ],
                "schema name `T` is shared",
            ),
            (
                "view NULL sql",
                vec![implicit_autoindex_catalog_row("view", "v", "v", 0, None)],
                "non-NULL sql",
            ),
            (
                "view name mismatch",
                vec![implicit_autoindex_catalog_row(
                    "view",
                    "v",
                    "v",
                    0,
                    Some("CREATE VIEW other AS SELECT 1"),
                )],
                "differently named view",
            ),
            (
                "temporary view",
                vec![implicit_autoindex_catalog_row(
                    "view",
                    "v",
                    "v",
                    0,
                    Some("CREATE TEMP VIEW v AS SELECT 1"),
                )],
                "temporary, non-main, or differently named view",
            ),
            (
                "missing trigger target",
                vec![implicit_autoindex_catalog_row(
                    "trigger",
                    "tr",
                    "missing",
                    0,
                    Some("CREATE TRIGGER tr AFTER INSERT ON missing BEGIN SELECT 1; END"),
                )],
                "missing or incompatible table `missing`",
            ),
            (
                "INSTEAD OF trigger on table",
                vec![
                    ordinary_table(),
                    implicit_autoindex_catalog_row(
                        "trigger",
                        "tr",
                        "t",
                        0,
                        Some("CREATE TRIGGER tr INSTEAD OF INSERT ON t BEGIN SELECT 1; END"),
                    ),
                ],
                "missing or incompatible view `t`",
            ),
            (
                "AFTER trigger on view",
                vec![
                    implicit_autoindex_catalog_row(
                        "view",
                        "v",
                        "v",
                        0,
                        Some("CREATE VIEW v AS SELECT 1"),
                    ),
                    implicit_autoindex_catalog_row(
                        "trigger",
                        "tr",
                        "v",
                        0,
                        Some("CREATE TRIGGER tr AFTER INSERT ON v BEGIN SELECT 1; END"),
                    ),
                ],
                "missing or incompatible table `v`",
            ),
            (
                "explicit index parse failure",
                vec![
                    ordinary_table(),
                    implicit_autoindex_catalog_row(
                        "index",
                        "idx_t",
                        "t",
                        3,
                        Some("CREATE INDEX idx_t ON"),
                    ),
                ],
                "could not parse CREATE INDEX",
            ),
            (
                "explicit index table mismatch",
                vec![
                    ordinary_table(),
                    implicit_autoindex_catalog_row(
                        "index",
                        "idx_t",
                        "t",
                        3,
                        Some("CREATE INDEX idx_t ON other(a)"),
                    ),
                ],
                "on table `other` instead of `t`",
            ),
            (
                "hidden logical autoindex name claimed explicitly",
                vec![
                    implicit_autoindex_catalog_row(
                        "table",
                        "wr",
                        "wr",
                        2,
                        Some("CREATE TABLE wr(pk TEXT PRIMARY KEY, u TEXT) WITHOUT ROWID"),
                    ),
                    implicit_autoindex_catalog_row(
                        "index",
                        "sqlite_autoindex_wr_1",
                        "wr",
                        3,
                        Some("CREATE INDEX sqlite_autoindex_wr_1 ON wr(u)"),
                    ),
                ],
                "collides with logical implicit index",
            ),
            (
                "conflicting rootpage-zero virtual tables",
                vec![
                    implicit_autoindex_catalog_row(
                        "table",
                        "vt",
                        "vt",
                        0,
                        Some("CREATE VIRTUAL TABLE vt USING fts5(body)"),
                    ),
                    implicit_autoindex_catalog_row(
                        "table",
                        "VT",
                        "VT",
                        0,
                        Some("CREATE VIRTUAL TABLE vt USING rtree(id, min_x, max_x)"),
                    ),
                ],
                "conflicting virtual-table entries",
            ),
            (
                "third duplicate virtual table",
                vec![
                    implicit_autoindex_catalog_row(
                        "table",
                        "vt",
                        "vt",
                        0,
                        Some("CREATE VIRTUAL TABLE vt USING fts5(body)"),
                    ),
                    implicit_autoindex_catalog_row(
                        "table",
                        "VT",
                        "VT",
                        0,
                        Some("CREATE VIRTUAL TABLE vt USING fts5(body)"),
                    ),
                    implicit_autoindex_catalog_row(
                        "table",
                        "vt",
                        "vt",
                        0,
                        Some("CREATE VIRTUAL TABLE vt USING fts5(body)"),
                    ),
                ],
                "conflicting virtual-table entries",
            ),
        ];

        for (case_name, entries, detail_needle) in cases {
            assert_implicit_autoindex_catalog_corrupt(case_name, &entries, detail_needle);
        }
    }

    #[test]
    fn implicit_autoindex_catalog_rejects_reserved_or_free_root_pages() {
        let table_at = |root_page| {
            vec![implicit_autoindex_catalog_row(
                "table",
                "t",
                "t",
                root_page,
                Some("CREATE TABLE t(a)"),
            )]
        };

        let lock_byte_page = fsqlite_pager::lock_byte_page(PageSize::DEFAULT);
        assert_implicit_autoindex_catalog_corrupt_with_page_bound(
            "lock-byte root",
            &table_at(i64::from(lock_byte_page)),
            lock_byte_page,
            "reserved lock-byte rootpage",
        );

        let auto_vacuum_header = DatabaseHeader {
            largest_root_page: 3,
            ..DatabaseHeader::default()
        };
        assert_implicit_autoindex_catalog_corrupt_with_root_context(
            "auto-vacuum pointer-map root",
            &table_at(2),
            3,
            &auto_vacuum_header,
            &HashSet::new(),
            "pointer-map rootpage 2",
        );

        let free_page = PageNumber::new(2).unwrap();
        assert_implicit_autoindex_catalog_corrupt_with_root_context(
            "freelist root",
            &table_at(2),
            2,
            &DatabaseHeader::default(),
            &HashSet::from([free_page]),
            "uses free rootpage 2",
        );
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

    /// GH #304: the physical index builder must honor declared per-term sort
    /// directions, not just echo them back into the `CREATE INDEX` DDL.
    ///
    /// Before the fix the builder inserted every index record through a plain
    /// ascending `BtCursor`, so a `DESC` term produced a b-tree whose physical
    /// order contradicted its own `sqlite_master` declaration. Stock SQLite
    /// reads the index with `DESC` comparison semantics, so it either reports
    /// the image as malformed or silently misses rows on a forced-index scan.
    #[test]
    fn test_persist_to_sqlite_builds_desc_index_in_declared_key_order() {
        asupersync::test_utils::run_test(|| async {
            const ROW_COUNT: i64 = 2_000;
            const GROUP_COUNT: i64 = 20;
            const PROBE_GROUP: i64 = 7;

            let dir = tempfile::tempdir().unwrap();
            let db_path = dir.path().join("desc-index-persist.db");
            let cx = Cx::new();

            let mut db = MemDatabase::new();
            db.create_table_at(2, 3);
            let table_data = db.get_table_mut(2).unwrap();
            for id in 1..=ROW_COUNT {
                table_data.insert_row(
                    id,
                    vec![
                        SqliteValue::Integer(id),
                        SqliteValue::Integer(id % GROUP_COUNT),
                        // Wide enough that the table b-tree spans many pages and
                        // the index spans multiple leaves under an interior node.
                        SqliteValue::Text(format!("payload-{id:0>200}").into()),
                    ],
                );
            }

            let schema = vec![TableSchema {
                name: "live".to_owned(),
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
                        name: "grp".to_owned(),
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
                        name: "payload".to_owned(),
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
                ],
                indexes: vec![IndexSchema {
                    name: "idx_live_grp_desc".to_owned(),
                    root_page: 3,
                    columns: vec!["grp".to_owned(), "id".to_owned()],
                    key_expressions: Vec::new(),
                    key_sort_directions: vec![SortDirection::Asc, SortDirection::Desc],
                    where_clause: None,
                    is_unique: false,
                    key_collations: vec![None, None],
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
                "live".to_owned(),
                "CREATE TABLE live (id INTEGER PRIMARY KEY, grp INTEGER NOT NULL, payload TEXT NOT NULL)"
                    .to_owned(),
            );
            original_ddl.insert(
                "idx_live_grp_desc".to_owned(),
                "CREATE INDEX idx_live_grp_desc ON live(grp, id DESC)".to_owned(),
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

            // Stock SQLite is the oracle here: it reads the index using the
            // DESC semantics declared in sqlite_master.
            let conn = rusqlite::Connection::open(&db_path).unwrap();

            let integrity: String = conn
                .query_row("PRAGMA integrity_check;", [], |row| row.get(0))
                .unwrap();
            assert_eq!(
                integrity, "ok",
                "stock SQLite must accept a persisted DESC index as structurally sound"
            );

            let declared_sql: String = conn
                .query_row(
                    "SELECT sql FROM sqlite_master WHERE type='index' AND name='idx_live_grp_desc';",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert!(
                declared_sql.to_ascii_lowercase().contains("id desc"),
                "persisted DDL must keep the DESC term: {declared_sql}"
            );

            let collect = |sql: &str| -> Vec<i64> {
                let mut stmt = conn.prepare(sql).unwrap();
                let rows = stmt
                    .query_map([PROBE_GROUP], |row| row.get::<_, i64>(0))
                    .unwrap()
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .unwrap();
                rows
            };

            let forced = collect(
                "SELECT id FROM live INDEXED BY idx_live_grp_desc \
                 WHERE grp = ?1 ORDER BY id DESC",
            );
            let scanned =
                collect("SELECT id FROM live NOT INDEXED WHERE grp = ?1 ORDER BY id DESC");

            assert!(
                !scanned.is_empty(),
                "probe group must actually contain rows"
            );
            assert_eq!(
                forced,
                scanned,
                "forced DESC-index lookup must return the same rows as the table scan \
                 (missing {} of {} rows)",
                scanned.len().saturating_sub(forced.len()),
                scanned.len()
            );
        });
    }

    /// GH #304 (collation + partial-index arm): a `DESC` term carrying a
    /// non-BINARY built-in collation must also be built in declared order.
    ///
    /// `NOCASE` is the discriminating case: under BINARY every uppercase
    /// prefix sorts before every lowercase one, while under NOCASE they
    /// interleave alphabetically. An index declared `COLLATE NOCASE DESC` but
    /// physically built BINARY/ASC is therefore doubly out of order, and stock
    /// SQLite rejects it. The partial predicate exercises the same builder
    /// loop with row filtering active.
    #[test]
    fn test_persist_to_sqlite_builds_collated_desc_partial_index_in_declared_order() {
        asupersync::test_utils::run_test(|| async {
            const ROW_COUNT: i64 = 400;

            let dir = tempfile::tempdir().unwrap();
            let db_path = dir.path().join("collated-desc-partial-index.db");
            let cx = Cx::new();

            // Case-varying prefixes: BINARY and NOCASE disagree on their order.
            let prefixes = ["a", "B", "c", "D"];

            let mut db = MemDatabase::new();
            db.create_table_at(2, 3);
            let table_data = db.get_table_mut(2).unwrap();
            for id in 1..=ROW_COUNT {
                let prefix = prefixes[usize::try_from(id - 1).unwrap() % prefixes.len()];
                table_data.insert_row(
                    id,
                    vec![
                        SqliteValue::Integer(id),
                        SqliteValue::Text(format!("{prefix}{id:04}").into()),
                        // Only two thirds of the rows satisfy the predicate.
                        SqliteValue::Integer(i64::from(id % 3 != 0)),
                    ],
                );
            }

            let schema = vec![TableSchema {
                name: "docs".to_owned(),
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
                        name: "active".to_owned(),
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
                    name: "idx_docs_name_ci_desc".to_owned(),
                    root_page: 3,
                    columns: vec!["name".to_owned()],
                    key_expressions: Vec::new(),
                    key_sort_directions: vec![SortDirection::Desc],
                    where_clause: Some("active = 1".to_owned()),
                    is_unique: false,
                    key_collations: vec![Some("NOCASE".to_owned())],
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
                "docs".to_owned(),
                "CREATE TABLE docs (id INTEGER PRIMARY KEY, name TEXT NOT NULL, active INTEGER NOT NULL DEFAULT 1)"
                    .to_owned(),
            );
            original_ddl.insert(
                "idx_docs_name_ci_desc".to_owned(),
                "CREATE INDEX idx_docs_name_ci_desc ON docs(name COLLATE NOCASE DESC) WHERE active = 1"
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
            assert_eq!(
                integrity, "ok",
                "stock SQLite must accept a persisted COLLATE NOCASE DESC partial index"
            );

            let collect = |sql: &str| -> Vec<String> {
                let mut stmt = conn.prepare(sql).unwrap();
                stmt.query_map([], |row| row.get::<_, String>(0))
                    .unwrap()
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .unwrap()
            };

            let forced = collect(
                "SELECT name FROM docs INDEXED BY idx_docs_name_ci_desc \
                 WHERE active = 1 ORDER BY name COLLATE NOCASE DESC",
            );
            let scanned = collect(
                "SELECT name FROM docs NOT INDEXED \
                 WHERE active = 1 ORDER BY name COLLATE NOCASE DESC",
            );

            assert!(
                !scanned.is_empty(),
                "partial predicate must retain some rows"
            );
            assert!(
                scanned.len() < usize::try_from(ROW_COUNT).unwrap(),
                "partial predicate must actually exclude some rows"
            );
            assert_eq!(
                forced, scanned,
                "forced collated-DESC partial-index scan must match the table scan"
            );
        });
    }

    /// GH #304 (UNIQUE + three-term mixed-direction + RTRIM arm): the facets
    /// `398bab01` explicitly left unverified.
    ///
    /// `398bab01` covered rowid tables, built-in collations, and single or
    /// composite ASC/DESC terms, but stated that the unique/auto arms,
    /// `quick_check`, and source-versus-candidate record parity were not
    /// covered. This exercises all of them at once:
    ///
    /// * a `UNIQUE` index, whose `is_unique` flag reaches only the regenerated
    ///   DDL and never the physical builder;
    /// * three key terms with mixed directions, so a single shared direction
    ///   flag cannot accidentally satisfy the ordering;
    /// * `RTRIM`, where `'c0007'` and `'c0007   '` compare equal but sort
    ///   differently from BINARY, so a builder that ignored the declared
    ///   collation would place trailing-space rows in the wrong leaf;
    /// * both `quick_check` and full `integrity_check` under stock SQLite;
    /// * index-versus-table row-count parity, which catches entries silently
    ///   dropped or duplicated during the rebuild.
    #[test]
    fn test_persist_to_sqlite_builds_unique_mixed_direction_rtrim_index() {
        asupersync::test_utils::run_test(|| async {
            const ROW_COUNT: i64 = 1_500;
            const CODE_GROUPS: i64 = 300;
            const TIER_COUNT: i64 = 7;

            let dir = tempfile::tempdir().unwrap();
            let db_path = dir.path().join("unique-mixed-rtrim.db");
            let cx = Cx::new();

            let text_column = |name: &str| ColumnInfo {
                name: name.to_owned(),
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
            };
            let int_column = |name: &str, is_ipk: bool| ColumnInfo {
                name: name.to_owned(),
                affinity: 'D',
                is_ipk,
                type_name: Some("INTEGER".to_owned()),
                notnull: false,
                unique: false,
                default_value: None,
                strict_type: None,
                generated_expr: None,
                generated_stored: None,
                collation: None,
                conflict_action: None,
            };

            let mut db = MemDatabase::new();
            db.create_table_at(2, 4);
            let table_data = db.get_table_mut(2).unwrap();
            for id in 1..=ROW_COUNT {
                // Trailing spaces must alternate across *recurrences of the same
                // base*, so the rule keys on the occurrence (the quotient), not
                // on `id`. Any rule of the form `id % k` where `k` divides
                // `CODE_GROUPS` gives every recurrence of a base an identical
                // spelling — the base repeats every `CODE_GROUPS` rows and
                // `id % k` is invariant under that step — so RTRIM would never
                // diverge from BINARY and a BINARY rebuild would satisfy the
                // ordering assertions below. Alternating by occurrence puts both
                // `c0007` and `c0007   ` in the table, which RTRIM folds together
                // and BINARY orders apart.
                let base = format!("c{:0>4}", id % CODE_GROUPS);
                let occurrence = id / CODE_GROUPS;
                let code = if occurrence % 2 == 1 {
                    format!("{base}   ")
                } else {
                    base
                };
                table_data.insert_row(
                    id,
                    vec![
                        SqliteValue::Integer(id),
                        SqliteValue::Text(code.into()),
                        SqliteValue::Integer(id % TIER_COUNT),
                        SqliteValue::Text(format!("note-{id:0>200}").into()),
                    ],
                );
            }

            let schema = vec![TableSchema {
                name: "catalog".to_owned(),
                root_page: 2,
                columns: vec![
                    int_column("id", true),
                    text_column("code"),
                    int_column("tier", false),
                    text_column("note"),
                ],
                indexes: vec![IndexSchema {
                    name: "idx_catalog_mixed".to_owned(),
                    root_page: 3,
                    columns: vec!["code".to_owned(), "tier".to_owned(), "id".to_owned()],
                    key_expressions: Vec::new(),
                    key_sort_directions: vec![
                        SortDirection::Asc,
                        SortDirection::Desc,
                        SortDirection::Asc,
                    ],
                    where_clause: None,
                    is_unique: true,
                    key_collations: vec![Some("RTRIM".to_owned()), None, None],
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
                "catalog".to_owned(),
                "CREATE TABLE catalog (id INTEGER PRIMARY KEY, code TEXT, tier INTEGER, note TEXT)"
                    .to_owned(),
            );
            original_ddl.insert(
                "idx_catalog_mixed".to_owned(),
                "CREATE UNIQUE INDEX idx_catalog_mixed \
                 ON catalog(code COLLATE RTRIM, tier DESC, id)"
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

            let quick: String = conn
                .query_row("PRAGMA quick_check;", [], |row| row.get(0))
                .unwrap();
            assert_eq!(
                quick, "ok",
                "stock SQLite quick_check must accept the rebuilt UNIQUE index"
            );
            let integrity: String = conn
                .query_row("PRAGMA integrity_check;", [], |row| row.get(0))
                .unwrap();
            assert_eq!(
                integrity, "ok",
                "stock SQLite integrity_check must accept the rebuilt UNIQUE index"
            );

            let declared_sql: String = conn
                .query_row(
                    "SELECT sql FROM sqlite_master \
                     WHERE type='index' AND name='idx_catalog_mixed';",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            let lowered = declared_sql.to_ascii_lowercase();
            assert!(
                lowered.contains("unique") && lowered.contains("rtrim") && lowered.contains("desc"),
                "persisted DDL must keep UNIQUE, RTRIM and DESC: {declared_sql}"
            );

            // Every table row must be reachable through the index; a dropped or
            // duplicated entry shows up here even when integrity_check passes.
            let indexed_count: i64 = conn
                .query_row(
                    "SELECT count(*) FROM catalog INDEXED BY idx_catalog_mixed;",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            let scanned_count: i64 = conn
                .query_row("SELECT count(*) FROM catalog NOT INDEXED;", [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(scanned_count, ROW_COUNT, "fixture must persist every row");
            assert_eq!(
                indexed_count, scanned_count,
                "index must contain exactly one entry per table row"
            );

            // Compare over ALL rows rather than a `tier` cohort. Same-base rows
            // recur every `CODE_GROUPS` ids, and `CODE_GROUPS % TIER_COUNT` is 6,
            // so the five occurrences of a base land on five *distinct* tiers:
            // any `WHERE tier = ?` cohort therefore contains at most one row per
            // base, no trimmed/untrimmed pair survives the filter, and RTRIM and
            // BINARY can agree on the filtered set. An unfiltered traversal keeps
            // both spellings of every base in the comparison and is also the most
            // direct exercise of the whole declared key shape — ASC+RTRIM, then
            // DESC, then ASC.
            let collect = |sql: &str| -> Vec<i64> {
                let mut stmt = conn.prepare(sql).unwrap();
                stmt.query_map([], |row| row.get::<_, i64>(0))
                    .unwrap()
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .unwrap()
            };

            // The fixture must actually be able to tell RTRIM from BINARY,
            // otherwise the ordering assertion below passes for an index that
            // was rebuilt with the wrong collation. Prove it on the table scan,
            // where neither ordering can come from the index under test: if the
            // two collations agree on this data the fixture is not
            // discriminatory and the rest of this test proves nothing.
            let rtrim_scan = collect(
                "SELECT id FROM catalog NOT INDEXED \
                 ORDER BY code COLLATE RTRIM, tier DESC, id",
            );
            let binary_scan = collect(
                "SELECT id FROM catalog NOT INDEXED \
                 ORDER BY code COLLATE BINARY, tier DESC, id",
            );
            assert_ne!(
                rtrim_scan, binary_scan,
                "fixture must distinguish RTRIM from BINARY, otherwise a BINARY \
                 rebuild would satisfy the index-order assertion below"
            );
            let both_spellings: i64 = conn
                .query_row(
                    "SELECT count(*) FROM (SELECT rtrim(code) AS b FROM catalog \
                     GROUP BY b HAVING count(DISTINCT code) > 1);",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert!(
                both_spellings > 0,
                "at least one base must appear both trimmed and untrimmed"
            );

            let forced = collect(
                "SELECT id FROM catalog INDEXED BY idx_catalog_mixed \
                 ORDER BY code COLLATE RTRIM, tier DESC, id",
            );
            assert_eq!(
                forced.len(),
                usize::try_from(ROW_COUNT).unwrap(),
                "the forced index traversal must visit every row"
            );
            assert_eq!(
                forced, rtrim_scan,
                "forced UNIQUE mixed-direction RTRIM index traversal must match the table scan"
            );
        });
    }

    /// GH #304 (custom-collation arm): an index whose declared collation is not
    /// resolvable by the builder must be refused, not rebuilt under BINARY.
    ///
    /// The source connection's collation registry is not reachable from the
    /// persist path, so a `COLLATE MYCOLL` term previously fell back to BINARY
    /// while the regenerated DDL kept saying `MYCOLL`. That produces the same
    /// malformed-image class GH #304 was filed for, minus the DESC symptom that
    /// made the original report visible — a silently wrong ordering rather than
    /// a loud one. Refusing keeps the source image intact and surfaces the gap.
    ///
    /// Deliberately NOT covered here, and still open on GH #304:
    ///
    /// * **Built-in name override.** A source connection may register its own
    ///   implementation under `BINARY`/`NOCASE`/`RTRIM`. The guard sees the name
    ///   present and admits the index, which is then built with the *default*
    ///   implementation — silently mis-ordered. No test asserts this, because
    ///   provoking it means mutating the process-wide default registry, which
    ///   would leak into every other test in this binary (the exact global-state
    ///   hazard tracked by GH #299). It needs registry identity, not a name.
    /// * **Candidate cleanup.** The `db_path.exists()` assertion below documents
    ///   *ownership* only — it pins that this function does not delete its own
    ///   output. It is not a cleanup proof. The release evidence still requires a
    ///   keeper over the enclosing `VacuumTargetReservation` in `vacuum.rs`
    ///   showing the partial candidate is actually removed on this failure path
    ///   and that no caller-owned path is touched.
    #[test]
    fn test_persist_to_sqlite_refuses_unresolvable_index_collation() {
        asupersync::test_utils::run_test(|| async {
            let dir = tempfile::tempdir().unwrap();
            let db_path = dir.path().join("unresolvable-collation.db");
            let cx = Cx::new();

            let mut db = MemDatabase::new();
            db.create_table_at(2, 2);
            let table_data = db.get_table_mut(2).unwrap();
            for id in 1..=8_i64 {
                table_data.insert_row(
                    id,
                    vec![
                        SqliteValue::Integer(id),
                        SqliteValue::Text(format!("v{id}").into()),
                    ],
                );
            }

            let schema = vec![TableSchema {
                name: "t".to_owned(),
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
                        name: "label".to_owned(),
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
                    name: "idx_t_label_custom".to_owned(),
                    root_page: 3,
                    columns: vec!["label".to_owned()],
                    key_expressions: Vec::new(),
                    key_sort_directions: vec![SortDirection::Asc],
                    where_clause: None,
                    is_unique: false,
                    key_collations: vec![Some("MYCOLL".to_owned())],
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
                "t".to_owned(),
                "CREATE TABLE t (id INTEGER PRIMARY KEY, label TEXT)".to_owned(),
            );
            original_ddl.insert(
                "idx_t_label_custom".to_owned(),
                "CREATE INDEX idx_t_label_custom ON t(label COLLATE MYCOLL)".to_owned(),
            );

            let error = persist_to_sqlite_with_header_and_master_entries(
                &cx,
                &db_path,
                &schema,
                &db,
                &header,
                &[],
                &original_ddl,
            )
            .await
            .expect_err("an unresolvable index collation must fail closed");
            let rendered = error.to_string();
            assert!(
                rendered.contains("MYCOLL") && rendered.contains("contradict its own declaration"),
                "refusal must name the unresolvable collation and why it is refused: {rendered}"
            );
            // A legitimate schema this builder cannot honour is a supported-
            // schema limitation, not a violated internal invariant.
            assert!(
                matches!(error, FrankenError::NotImplemented(_)),
                "refusal must be typed as NotImplemented, found {error:?}"
            );

            // Candidate cleanup is deliberately NOT this function's job: it never
            // removes its own output on any error path, and the enclosing VACUUM
            // caller owns removal through its identity-bound
            // `VacuumTargetReservation`. Pin the actual post-failure state so a
            // future change to that ownership boundary is caught here rather than
            // silently leaking or silently starting to delete caller-owned paths.
            let candidate_exists_after_refusal = db_path.exists();

            // A built-in collation on the same shape must still succeed, so the
            // guard rejects only what it genuinely cannot order. The DDL must be
            // regenerated to match: reusing the MYCOLL text would persist an
            // index whose declaration contradicts the key metadata actually
            // built, which is the very defect this test exists to prevent, and
            // stock SQLite would reject the unknown collation on open.
            let mut ok_schema = schema;
            ok_schema[0].indexes[0].key_collations = vec![Some("NOCASE".to_owned())];
            let mut ok_ddl = HashMap::new();
            ok_ddl.insert(
                "t".to_owned(),
                "CREATE TABLE t (id INTEGER PRIMARY KEY, label TEXT)".to_owned(),
            );
            ok_ddl.insert(
                "idx_t_label_custom".to_owned(),
                "CREATE INDEX idx_t_label_custom ON t(label COLLATE NOCASE)".to_owned(),
            );
            let ok_path = dir.path().join("resolvable-collation.db");
            persist_to_sqlite_with_header_and_master_entries(
                &cx,
                &ok_path,
                &ok_schema,
                &db,
                &header,
                &[],
                &ok_ddl,
            )
            .await
            .expect("a built-in collation must still rebuild");
            let conn = rusqlite::Connection::open(&ok_path).unwrap();
            let integrity: String = conn
                .query_row("PRAGMA integrity_check;", [], |row| row.get(0))
                .unwrap();
            assert_eq!(integrity, "ok");
            let declared: String = conn
                .query_row(
                    "SELECT sql FROM sqlite_master \
                     WHERE type='index' AND name='idx_t_label_custom';",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert!(
                declared.to_ascii_uppercase().contains("NOCASE")
                    && !declared.to_ascii_uppercase().contains("MYCOLL"),
                "persisted DDL must declare the collation actually built: {declared}"
            );

            // Reported after the success path so a leak is described precisely
            // rather than aborting the more informative assertions above.
            assert!(
                candidate_exists_after_refusal,
                "refusal leaves the partial candidate for the caller's reservation to remove; \
                 if this now fails, cleanup ownership moved into the persist path and the \
                 comment above it must be updated"
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
    fn test_load_from_sqlite_rejects_explicit_index_with_missing_key_column() {
        asupersync::test_utils::run_test(|| async {
            let dir = tempfile::tempdir().unwrap();
            let db_path = dir.path().join("compat_corrupt_index_key_column.db");

            {
                let sqlite = rusqlite::Connection::open(&db_path).unwrap();
                sqlite
                    .execute_batch(
                        r"
                CREATE TABLE docs (id INTEGER PRIMARY KEY, title TEXT);
                CREATE INDEX docs_title_idx ON docs(title);
                PRAGMA writable_schema = ON;
                UPDATE sqlite_master
                SET sql = 'CREATE INDEX docs_title_idx ON docs(missing_title)'
                WHERE name = 'docs_title_idx';
                PRAGMA writable_schema = OFF;
                ",
                    )
                    .unwrap();
            }

            let error = load_test_db(&db_path)
                .await
                .expect_err("compat reload must reject an unresolved explicit-index key");
            assert!(matches!(&error, FrankenError::DatabaseCorrupt { .. }));
            let message = error.to_string();
            assert!(
                message.contains("docs_title_idx") && message.contains("missing_title"),
                "unexpected malformed-index compat-load error: {message}"
            );
        });
    }

    #[test]
    fn test_load_from_sqlite_rejects_schema_qualified_persisted_create_index_sql() {
        asupersync::test_utils::run_test(|| async {
            let dir = tempfile::tempdir().unwrap();
            let db_path = dir.path().join("compat_schema_qualified_index_sql.db");
            {
                let sqlite = rusqlite::Connection::open(&db_path).unwrap();
                sqlite
                    .execute_batch(
                        r"
                CREATE TABLE docs (id INTEGER PRIMARY KEY, title TEXT);
                CREATE INDEX docs_title_idx ON docs(title);
                PRAGMA writable_schema = ON;
                UPDATE sqlite_master
                SET sql = 'CREATE INDEX main.docs_title_idx ON docs(title)'
                WHERE name = 'docs_title_idx';
                PRAGMA writable_schema = OFF;
                ",
                    )
                    .unwrap();
            }

            let error = load_test_db(&db_path)
                .await
                .expect_err("compat load must reject schema-qualified index SQL");
            assert!(matches!(&error, FrankenError::DatabaseCorrupt { .. }));
            let message = error.to_string();
            assert!(
                message.contains("docs_title_idx") && message.contains("schema-qualified"),
                "unexpected schema-qualified-index compat-load error: {message}"
            );
        });
    }

    #[test]
    fn test_load_from_sqlite_rejects_known_invalid_index_functions() {
        asupersync::test_utils::run_test(|| async {
            let dir = tempfile::tempdir().unwrap();
            for (case, create_sql, expected) in [
                (
                    "random",
                    "CREATE INDEX docs_title_idx ON docs(random())",
                    "non-deterministic",
                ),
                (
                    "aggregate",
                    "CREATE INDEX docs_title_idx ON docs(sum(title))",
                    "aggregate",
                ),
                (
                    "wrong_arity",
                    "CREATE INDEX docs_title_idx ON docs(lower(title, title))",
                    "wrong number of arguments",
                ),
                (
                    "current_timestamp",
                    "CREATE INDEX docs_title_idx ON docs(CURRENT_TIMESTAMP)",
                    "non-deterministic",
                ),
            ] {
                let db_path = dir
                    .path()
                    .join(format!("compat_invalid_index_function_{case}.db"));
                {
                    let sqlite = rusqlite::Connection::open(&db_path).unwrap();
                    sqlite
                        .execute_batch(
                            r"
                        CREATE TABLE docs (id INTEGER PRIMARY KEY, title TEXT);
                        CREATE INDEX docs_title_idx ON docs(title);
                        PRAGMA writable_schema = ON;
                        ",
                        )
                        .unwrap();
                    sqlite
                        .execute(
                            "UPDATE sqlite_master SET sql = ?1 WHERE name = 'docs_title_idx'",
                            [create_sql],
                        )
                        .unwrap();
                    sqlite
                        .execute_batch("PRAGMA writable_schema = OFF;")
                        .unwrap();
                }

                let error = load_test_db(&db_path)
                    .await
                    .expect_err("known-invalid persisted index function must fail compat load");
                assert!(matches!(&error, FrankenError::DatabaseCorrupt { .. }));
                let message = error.to_string().to_ascii_lowercase();
                assert!(
                    message.contains("docs_title_idx") && message.contains(expected),
                    "unexpected compat persisted-function error for `{create_sql}`: {error}"
                );
            }
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
