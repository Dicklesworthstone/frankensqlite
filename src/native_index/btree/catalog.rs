#![allow(clippy::future_not_send)]
//! Transactional schema/root catalog over the native page and B-tree path.
//!
//! Page 1 contains the existing 100-byte database header and a real table
//! B-tree of five-column SQLite schema records. No side metadata map, caller
//! remembered root, or new schema wire format is needed after recovery.
//!
//! This is the storage seam for a SQL executor, NOT a second SQL executor.
//! CREATE text is parsed to bind names, ownership and root kind. Expression
//! evaluation, constraints, automatic indexes and row population belong to the
//! caller's explicit builder. Its work and the catalog entry share one private
//! savepoint and the eventual NativePageStore commit. File names, namespace
//! admission and cross-process ownership still belong to the native log owner.

use std::collections::BTreeSet;
use std::ops::AsyncFnOnce;

use fsqlite_ast::{CreateTableBody, Statement};
use fsqlite_btree::{BtCursor, BtreeCursorOps};
use fsqlite_error::{FrankenError, Result};
use fsqlite_parser::{Lexer, Parser};
use fsqlite_types::cx::Cx;
use fsqlite_types::record::{parse_record, serialize_record};
use fsqlite_types::{
    DATABASE_HEADER_SIZE, DatabaseHeader, PageNumber, PageSize, SqliteValue, TextEncoding,
};
use fsqlite_vfs::VfsFile;
use fsqlite_wal::native_commit::durable::NativeObjectCodec;
use fsqlite_wal::native_pages::{NativePageStore, NativePageTransaction};

use super::{MutationScope, NativeBtreePageIo, create_native_btree, malformed, with_native_btree};

/// Admission bounds, also enforced when reading persisted catalog records.
pub const MAX_NATIVE_SCHEMA_OBJECTS: usize = 1024;
pub const MAX_NATIVE_SCHEMA_SQL_BYTES: usize = 16 * 1024;
pub const MAX_NATIVE_SCHEMA_BYTES: usize = 1024 * 1024;
const MAX_NAME_BYTES: usize = 1024;

/// Root-bearing schema objects supported by this storage profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeSchemaKind {
    Table,
    Index,
}

impl NativeSchemaKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Table => "table",
            Self::Index => "index",
        }
    }
}

/// One decoded sqlite_schema row. Fields are immutable to consumers; mutation
/// must go through a transaction rather than changing a cached root descriptor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeSchemaEntry {
    rowid: i64,
    kind: NativeSchemaKind,
    name: String,
    table_name: String,
    root: PageNumber,
    sql: String,
    table_btree: bool,
}

impl NativeSchemaEntry {
    #[must_use]
    pub const fn rowid(&self) -> i64 { self.rowid }
    #[must_use]
    pub const fn kind(&self) -> NativeSchemaKind { self.kind }
    #[must_use]
    pub fn name(&self) -> &str { &self.name }
    #[must_use]
    pub fn table_name(&self) -> &str { &self.table_name }
    #[must_use]
    pub const fn root(&self) -> PageNumber { self.root }
    #[must_use]
    pub fn sql(&self) -> &str { &self.sql }
    /// WITHOUT ROWID tables, like indexes, use a blob-key B-tree.
    #[must_use]
    pub const fn is_table_btree(&self) -> bool { self.table_btree }

    // The native catalog is this engine's own format, always UTF-8 text in
    // canonical records; it never carries a database's UTF-16 encoding.
    #[allow(clippy::disallowed_methods)]
    fn record(&self) -> Vec<u8> {
        serialize_record(&[
            SqliteValue::Text(self.kind.as_str().into()),
            SqliteValue::Text(self.name.clone().into()),
            SqliteValue::Text(self.table_name.clone().into()),
            SqliteValue::Integer(i64::from(self.root.get())),
            SqliteValue::Text(self.sql.clone().into()),
        ])
    }

    #[allow(clippy::disallowed_methods)] // canonical UTF-8 native record, see `record`
    fn decode(rowid: i64, bytes: &[u8]) -> Result<Self> {
        if rowid <= 0 || bytes.len() > MAX_NATIVE_SCHEMA_SQL_BYTES + 3 * MAX_NAME_BYTES + 64 {
            return Err(malformed("native schema record exceeds its bounds"));
        }
        let fields = parse_record(bytes).ok_or_else(|| malformed("malformed native schema record"))?;
        let [SqliteValue::Text(kind), SqliteValue::Text(name), SqliteValue::Text(table),
            SqliteValue::Integer(root), SqliteValue::Text(sql)] = fields.as_slice()
        else {
            return Err(malformed("native schema requires five typed fields"));
        };
        let sql = sql.to_string();
        let shape = Definition::parse(&sql)
            .map_err(|_| malformed("invalid CREATE text in native schema"))?;
        if kind.to_string() != shape.kind.as_str() || name.to_string() != shape.name
            || !table.to_string().eq_ignore_ascii_case(&shape.table_name)
        {
            return Err(malformed("native schema fields do not match their CREATE text"));
        }
        let root = u32::try_from(*root).ok().and_then(PageNumber::new)
            .filter(|root| root.get() > 1)
            .ok_or_else(|| malformed("native schema object has an invalid root"))?;
        let entry = Self {
            rowid, kind: shape.kind, name: shape.name, table_name: table.to_string(),
            root, sql, table_btree: shape.table_btree,
        };
        // This native profile writes canonical records. Re-encoding also
        // detects trailing bytes or lossy text decoding in the general parser.
        if entry.record() != bytes {
            return Err(malformed("noncanonical native schema record"));
        }
        Ok(entry)
    }
}

/// Snapshot-local metadata, not a live mutable cache or an authority token.
///
/// Named tree access resolves again through the supplied transaction, so a
/// descriptor saved by an older transaction cannot silently select today's root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeSchemaSnapshot {
    cookie: u32,
    entries: Vec<NativeSchemaEntry>,
    encoded_bytes: usize,
}

impl NativeSchemaSnapshot {
    #[must_use]
    pub const fn cookie(&self) -> u32 { self.cookie }
    #[must_use]
    pub fn entries(&self) -> &[NativeSchemaEntry] { &self.entries }
    #[must_use]
    pub fn find(&self, name: &str) -> Option<&NativeSchemaEntry> {
        self.entries.iter().find(|entry| entry.name.eq_ignore_ascii_case(name))
    }
}

struct Definition {
    kind: NativeSchemaKind,
    name: String,
    table_name: String,
    table_btree: bool,
}

impl Definition {
    fn parse(sql: &str) -> Result<Self> {
        if sql.is_empty() || sql.len() > MAX_NATIVE_SCHEMA_SQL_BYTES || sql.contains('\0') {
            return Err(FrankenError::TooBig);
        }
        let (statements, errors) = Parser::new(Lexer::tokenize(sql)).parse_all();
        if !errors.is_empty() || statements.len() != 1 {
            return Err(malformed("native schema requires exactly one valid CREATE statement"));
        }
        let (definition, schema) = match &statements[0] {
            Statement::CreateTable(table) if !table.temporary
                && matches!(table.body, CreateTableBody::Columns { .. }) => (
                Self {
                    kind: NativeSchemaKind::Table,
                    name: table.name.name.clone(), table_name: table.name.name.clone(),
                    table_btree: !table.without_rowid,
                },
                &table.name.schema,
            ),
            Statement::CreateIndex(index) => (
                Self {
                    kind: NativeSchemaKind::Index,
                    name: index.name.name.clone(), table_name: index.table.clone(),
                    table_btree: false,
                },
                &index.name.schema,
            ),
            _ => return Err(FrankenError::Unsupported),
        };
        if schema.as_ref().is_some_and(|schema| !schema.eq_ignore_ascii_case("main")) {
            return Err(FrankenError::Unsupported);
        }
        validate_name(&definition.name)?;
        validate_name(&definition.table_name)?;
        Ok(definition)
    }
}

fn validate_name(name: &str) -> Result<()> {
    if name.is_empty() || name.len() > MAX_NAME_BYTES || name.contains('\0')
        || name.get(..7).is_some_and(|prefix| prefix.eq_ignore_ascii_case("sqlite_"))
    {
        return Err(malformed("invalid or reserved native schema object name"));
    }
    Ok(())
}

fn schema_page() -> PageNumber { PageNumber::new(1).expect("page one is nonzero") }

fn read_header<S: VfsFile, M: VfsFile, C: NativeObjectCodec>(
    cx: &Cx, store: &NativePageStore<S, M, C>, txn: &mut NativePageTransaction,
) -> Result<DatabaseHeader> {
    let page = store.read_page(cx, txn, schema_page())?
        .ok_or_else(|| malformed("native schema has not been initialized"))?;
    let header_bytes: &[u8; DATABASE_HEADER_SIZE] = page.get(..DATABASE_HEADER_SIZE)
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(|| malformed("short native schema header"))?;
    let header = DatabaseHeader::from_bytes(header_bytes)
        .map_err(|error| malformed(&format!("invalid native schema header: {error}")))?;
    if header.page_size.get() != store.page_size() || header.reserved_per_page != 0
        || header.text_encoding != TextEncoding::Utf8 || header.text_encoding_unset
        || header.schema_format != 4 || header.freelist_trunk != 0 || header.freelist_count != 0
        || header.largest_root_page != 0 || header.incremental_vacuum != 0
        || header.read_version != 1 || header.write_version != 1
        || !matches!(page.get(DATABASE_HEADER_SIZE), Some(0x05 | 0x0D))
    {
        return Err(malformed("unsupported or inconsistent native schema header"));
    }
    Ok(header)
}

/// Initialize page 1's schema tree in a fresh native database transaction.
///
/// Repeated initialization validates existing metadata and leaves it untouched.
/// Missing page 1 in a non-genesis snapshot is corruption, never permission to
/// erase a schema. The caller commits explicitly; this creates no files.
///
/// The header's page count is intentionally unspecified: native page extent
/// comes from committed capsules, not from a compatibility main-file length.
///
/// # Errors
/// Returns owner/state, corruption, allocation or cancellation errors.
pub fn initialize_native_schema<S: VfsFile, M: VfsFile, C: NativeObjectCodec>(
    cx: &Cx, store: &NativePageStore<S, M, C>, txn: &mut NativePageTransaction,
) -> Result<bool> {
    if store.read_page(cx, txn, schema_page())?.is_some() {
        read_header(cx, store, txn)?;
        return Ok(false);
    }
    if txn.snapshot().get() != 0 {
        return Err(malformed("committed native database is missing its schema root"));
    }
    let header = DatabaseHeader {
        page_size: PageSize::new(store.page_size()).ok_or(FrankenError::TooBig)?,
        // A mismatched version-valid-for means a main-file page count is not
        // authoritative. Native commits do not dirty page 1 on every DML.
        change_counter: 1, version_valid_for: 0,
        ..DatabaseHeader::default()
    };
    let encoded = header.to_bytes().map_err(|error| malformed(&error.to_string()))?;
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(header.page_size.as_usize()).map_err(|_| FrankenError::OutOfMemory)?;
    bytes.resize(header.page_size.as_usize(), 0);
    bytes[..DATABASE_HEADER_SIZE].copy_from_slice(&encoded);
    bytes[DATABASE_HEADER_SIZE] = 0x0D;
    let content_start = if store.page_size() == 65_536 { 0 } else {
        u16::try_from(store.page_size()).map_err(|_| FrankenError::TooBig)?
    };
    bytes[DATABASE_HEADER_SIZE + 5..DATABASE_HEADER_SIZE + 7]
        .copy_from_slice(&content_start.to_be_bytes());
    store.write_page(cx, txn, schema_page(), Some(&bytes))?;
    Ok(true)
}

/// Read schema rows from the real catalog B-tree at `txn`'s snapshot.
///
/// Only catalog pages are read here, not every table root: independent data
/// writers must not become coupled by a catalog-wide scan of mutable roots.
/// Roots are checked when opened by name. Names and roots must be unique, and
/// each index must refer to a table present in this same schema snapshot.
///
/// # Errors
/// Rejects malformed metadata, missing owners, bounds and cursor/I/O failures.
pub async fn read_native_schema<S: VfsFile, M: VfsFile, C: NativeObjectCodec>(
    cx: &Cx, store: &NativePageStore<S, M, C>, txn: &mut NativePageTransaction,
) -> Result<NativeSchemaSnapshot> {
    let header = read_header(cx, store, txn)?;
    let (entries, encoded_bytes) = with_native_btree(cx, store, txn, schema_page(), true,
        async |cursor| {
            let mut entries = Vec::new();
            let mut encoded_bytes = 0_usize;
            let mut previous = 0_i64;
            if cursor.first(cx).await? {
                loop {
                    cx.checkpoint().map_err(|_| FrankenError::Interrupt)?;
                    if entries.len() == MAX_NATIVE_SCHEMA_OBJECTS { return Err(FrankenError::TooBig); }
                    let rowid = cursor.rowid(cx).await?;
                    if rowid <= previous { return Err(malformed("unordered native schema rowids")); }
                    // Inspect a bounded prefix before allocating an overflow
                    // payload. A malformed catalog cannot ask us to materialize
                    // an unbounded record just to reject its SQL afterward.
                    let mut bytes = Vec::new();
                    cursor.payload_prefix_into(cx, MAX_NATIVE_SCHEMA_SQL_BYTES + 3 * MAX_NAME_BYTES + 65, &mut bytes).await?;
                    encoded_bytes = encoded_bytes.checked_add(bytes.len())
                        .filter(|n| *n <= MAX_NATIVE_SCHEMA_BYTES).ok_or(FrankenError::TooBig)?;
                    entries.try_reserve(1).map_err(|_| FrankenError::OutOfMemory)?;
                    entries.push(NativeSchemaEntry::decode(rowid, &bytes)?);
                    previous = rowid;
                    if !cursor.next(cx).await? { break; }
                }
            }
            Ok((entries, encoded_bytes))
        },
    ).await?;
    let mut names = BTreeSet::new();
    let mut roots = BTreeSet::new();
    for entry in &entries {
        if !names.insert(entry.name.to_ascii_lowercase()) || !roots.insert(entry.root) {
            return Err(malformed("duplicate native schema name or root"));
        }
        if entry.kind == NativeSchemaKind::Index && !entries.iter().any(|table| {
            table.kind == NativeSchemaKind::Table && table.name.eq_ignore_ascii_case(&entry.table_name)
        }) {
            return Err(malformed("native schema index has no owning table"));
        }
    }
    Ok(NativeSchemaSnapshot { cookie: header.schema_cookie, entries, encoded_bytes })
}

#[allow(clippy::await_holding_refcell_ref)] // Private outer scope cannot be re-entered.
async fn edit<S, M, C, F, T>(
    cx: &Cx, store: &NativePageStore<S, M, C>, txn: &mut NativePageTransaction, operation: F,
) -> Result<T>
where
    S: VfsFile, M: VfsFile, C: NativeObjectCodec,
    F: for<'a> AsyncFnOnce(&'a mut NativePageTransaction) -> Result<T>,
{
    let scope = MutationScope::new(cx, store, txn)?;
    // This is a private outer transaction scope. Nested cursor scopes get the
    // borrowed transaction, never this RefCell, so no borrow can re-enter it.
    // On future drop the borrow retires before the scope restores its savepoint.
    let result = {
        let mut txn = scope.state.txn.try_borrow_mut().map_err(|_| FrankenError::Abort)?;
        operation(&mut txn).await
    };
    scope.finish(result)
}

fn bump_cookie<S: VfsFile, M: VfsFile, C: NativeObjectCodec>(
    cx: &Cx, store: &NativePageStore<S, M, C>, txn: &mut NativePageTransaction,
) -> Result<()> {
    // Read again after cursor edits: writing an old copy of page 1 would erase
    // the schema cells or root split that just occurred.
    let mut header = read_header(cx, store, txn)?;
    header.schema_cookie = header.schema_cookie.checked_add(1).ok_or(FrankenError::DatabaseFull)?;
    let header_bytes = header.to_bytes().map_err(|error| malformed(&error.to_string()))?;
    let page = store.read_page(cx, txn, schema_page())?
        .ok_or_else(|| malformed("native schema disappeared during mutation"))?;
    let mut bytes = page.to_vec();
    bytes[..DATABASE_HEADER_SIZE].copy_from_slice(&header_bytes);
    store.write_page(cx, txn, schema_page(), Some(&bytes))
}

/// Register a parsed table/index definition and initialize its fresh root.
///
/// The builder is mandatory: the SQL executor must populate data/index keys
/// and enforce the definition's semantics. No CREATE statement is executed by
/// this function, and IF NOT EXISTS does not suppress duplicate admission.
///
/// Catalog row, schema cookie, root and builder mutations are one savepoint.
/// The new row is visible only to this transaction while the builder runs;
/// an error, panic or dropped future restores ALL of those changes. Earlier
/// private work and read dependencies survive. No I/O commit happens here.
///
/// # Errors
/// Returns duplicate/missing-owner errors, parser/profile/bounds failures and
/// the original builder error. A successful result still requires store commit.
pub async fn create_native_schema_tree<S, M, C, F>(
    cx: &Cx, store: &NativePageStore<S, M, C>, txn: &mut NativePageTransaction,
    sql: &str, build: F,
) -> Result<NativeSchemaEntry>
where
    S: VfsFile, M: VfsFile, C: NativeObjectCodec,
    F: for<'a> AsyncFnOnce(&'a mut NativePageTransaction, &'a NativeSchemaEntry) -> Result<()>,
{
    let definition = Definition::parse(sql)?;
    edit(cx, store, txn, async move |txn| {
        let catalog = read_native_schema(cx, store, txn).await?;
        if catalog.find(&definition.name).is_some() {
            return Err(match definition.kind {
                NativeSchemaKind::Table => FrankenError::TableExists { name: definition.name.clone() },
                NativeSchemaKind::Index => FrankenError::IndexExists { name: definition.name.clone() },
            });
        }
        if catalog.entries.len() == MAX_NATIVE_SCHEMA_OBJECTS { return Err(FrankenError::TooBig); }
        let table_name = match definition.kind {
            NativeSchemaKind::Table => definition.name.clone(),
            NativeSchemaKind::Index => catalog.find(&definition.table_name)
                .filter(|owner| owner.kind == NativeSchemaKind::Table)
                .ok_or_else(|| FrankenError::NoSuchTable { name: definition.table_name.clone() })?
                .name.clone(),
        };
        let rowid = catalog.entries.last().map_or(0, |entry| entry.rowid)
            .checked_add(1).ok_or(FrankenError::DatabaseFull)?;
        let root = create_native_btree(cx, store, txn, definition.table_btree)?;
        let entry = NativeSchemaEntry {
            rowid, kind: definition.kind, name: definition.name, table_name,
            root, sql: sql.to_owned(), table_btree: definition.table_btree,
        };
        let record = entry.record();
        if catalog.encoded_bytes.checked_add(record.len()).is_none_or(|n| n > MAX_NATIVE_SCHEMA_BYTES) {
            return Err(FrankenError::TooBig);
        }
        with_native_btree(cx, store, txn, schema_page(), true, async |cursor| {
            cursor.table_insert(cx, rowid, &record).await
        }).await?;
        bump_cookie(cx, store, txn)?;
        build(txn, &entry).await?;
        // A builder may add dependent schema objects, but it must not replace
        // or remove the entry it was asked to populate.
        let final_catalog = read_native_schema(cx, store, txn).await?;
        if final_catalog.find(&entry.name) != Some(&entry) {
            return Err(malformed("native schema builder replaced its own catalog entry"));
        }
        with_native_btree(cx, store, txn, entry.root, entry.table_btree,
            async |_| Ok(())).await?;
        Ok(entry)
    }).await
}

/// Resolve a named root in the transaction's schema snapshot, then run the
/// existing B-tree cursor.
///
/// Every access records a catalog dependency as well
/// as traversal pages. Old snapshots resolve old names/roots, while a writer
/// depending on a concurrently changed schema is refused at commit.
///
/// # Errors
/// Returns missing/wrong-kind objects, malformed roots and callback/I/O errors.
pub async fn with_native_schema_tree<'a, S, M, C, F, T>(
    cx: &Cx, store: &'a NativePageStore<S, M, C>, txn: &'a mut NativePageTransaction,
    name: &str, kind: NativeSchemaKind, operation: F,
) -> Result<T>
where
    S: VfsFile, M: VfsFile, C: NativeObjectCodec,
    F: for<'b> AsyncFnOnce(&'b mut BtCursor<NativeBtreePageIo<'a, S, M, C>>) -> Result<T>,
{
    let catalog = read_native_schema(cx, store, txn).await?;
    let entry = catalog.find(name).filter(|entry| entry.kind == kind)
        .ok_or_else(|| match kind {
            NativeSchemaKind::Table => FrankenError::NoSuchTable { name: name.to_owned() },
            NativeSchemaKind::Index => FrankenError::NoSuchIndex { name: name.to_owned() },
        })?;
    with_native_btree(cx, store, txn, entry.root, entry.table_btree, operation).await
}

/// Execute a multi-tree statement in one rollback-on-drop private savepoint.
///
/// A SQL executor can update the table and all of its indexes here without
/// leaving an accepted table edit behind when a later index constraint fails.
/// Callback errors must be propagated; successful completion accepts the
/// overlay but does not publish a commit. Previously staged statements survive
/// failure, and read observations remain conservative across rollback.
///
/// # Errors
/// Returns schema/state/cancellation errors or the original callback error.
/// Panics and abandoned futures also restore the private statement overlay.
pub async fn with_native_schema_statement<S, M, C, F, T>(
    cx: &Cx, store: &NativePageStore<S, M, C>, txn: &mut NativePageTransaction,
    operation: F,
) -> Result<T>
where
    S: VfsFile, M: VfsFile, C: NativeObjectCodec,
    F: for<'a> AsyncFnOnce(&'a mut NativePageTransaction) -> Result<T>,
{
    edit(cx, store, txn, async move |txn| {
        read_native_schema(cx, store, txn).await?;
        let result = operation(txn).await?;
        read_native_schema(cx, store, txn).await?;
        Ok(result)
    }).await
}

/// Remove a table and its catalog-owned indexes, or just the named index.
///
/// Catalog rows, the schema cookie and versioned root deletions are atomic
/// with respect to the transaction. All roots are checked before any edit.
/// Old snapshots still resolve the previous schema and root/page versions;
/// new snapshots cannot discover the removed object. A name may subsequently
/// be registered again with a different, freshly allocated root.
///
/// This is logical schema retirement, not file deletion or space reclamation.
/// Descendant and historical pages remain retained for old snapshots and future
/// compaction. The SQL executor must perform trigger/foreign-key effects in a
/// surrounding statement scope; this function does not execute DROP SQL.
///
/// # Errors
/// Returns missing/wrong-kind objects, invalid roots, exhaustion, cancellation
/// and cursor errors. Partial catalog or root edits are restored on failure.
pub async fn drop_native_schema_tree<S: VfsFile, M: VfsFile, C: NativeObjectCodec>(
    cx: &Cx, store: &NativePageStore<S, M, C>, txn: &mut NativePageTransaction,
    name: &str, kind: NativeSchemaKind,
) -> Result<Vec<NativeSchemaEntry>> {
    edit(cx, store, txn, async |txn| {
        let catalog = read_native_schema(cx, store, txn).await?;
        let target = catalog.find(name).filter(|entry| entry.kind == kind)
            .ok_or_else(|| match kind {
                NativeSchemaKind::Table => FrankenError::NoSuchTable { name: name.to_owned() },
                NativeSchemaKind::Index => FrankenError::NoSuchIndex { name: name.to_owned() },
            })?;
        let mut removed = Vec::new();
        removed.try_reserve_exact(catalog.entries.len()).map_err(|_| FrankenError::OutOfMemory)?;
        for entry in &catalog.entries {
            if entry.rowid == target.rowid || (target.kind == NativeSchemaKind::Table
                && entry.kind == NativeSchemaKind::Index
                && entry.table_name.eq_ignore_ascii_case(&target.name))
            {
                with_native_btree(cx, store, txn, entry.root, entry.table_btree,
                    async |_| Ok(())).await?;
                removed.push(entry.clone());
            }
        }
        for entry in &removed {
            store.write_page(cx, txn, entry.root, None)?;
        }
        with_native_btree(cx, store, txn, schema_page(), true, async |cursor| {
            for entry in &removed {
                if !cursor.table_move_to(cx, entry.rowid).await?.is_found() {
                    return Err(malformed("native schema row disappeared during removal"));
                }
                cursor.delete(cx).await?;
            }
            Ok(())
        }).await?;
        bump_cookie(cx, store, txn)?;
        read_native_schema(cx, store, txn).await?;
        Ok(removed)
    }).await
}

#[cfg(test)]
#[allow(clippy::disallowed_methods)] // native catalog records are canonical UTF-8 by definition
mod tests {
    use super::*;

    fn entry() -> NativeSchemaEntry {
        NativeSchemaEntry {
            rowid: 1, kind: NativeSchemaKind::Table, name: "Items".to_owned(),
            table_name: "Items".to_owned(), root: PageNumber::new(2).unwrap(),
            sql: "CREATE TABLE Items(value TEXT)".to_owned(), table_btree: true,
        }
    }

    #[test]
    fn schema_record_uses_existing_sqlite_record_codec() {
        let entry = entry();
        let bytes = entry.record();
        assert_eq!(NativeSchemaEntry::decode(1, &bytes).unwrap(), entry);
        let values = parse_record(&bytes).unwrap();
        assert_eq!(values.len(), 5);
        assert_eq!(values[0], SqliteValue::Text("table".into()));
        assert_eq!(values[3], SqliteValue::Integer(2));
    }

    #[test]
    fn schema_record_rejects_metadata_substitution_and_reserved_roots() {
        for case in 0..6 {
            let mut value = entry();
            match case {
                0 => value.kind = NativeSchemaKind::Index,
                1 => value.name = "Other".to_owned(),
                2 => value.table_name = "Other".to_owned(),
                3 => value.root = schema_page(),
                4 => value.sql = "CREATE TABLE Other(value TEXT)".to_owned(),
                _ => value.sql = "SELECT 1".to_owned(),
            }
            assert!(NativeSchemaEntry::decode(1, &value.record()).is_err(), "case={case}");
        }
        assert!(NativeSchemaEntry::decode(0, &entry().record()).is_err());
    }

    #[test]
    fn schema_record_rejects_truncations_trailing_bytes_and_wrong_types() {
        let bytes = entry().record();
        for end in 0..bytes.len() { assert!(NativeSchemaEntry::decode(1, &bytes[..end]).is_err()); }
        let mut extra = bytes; extra.push(0);
        assert!(NativeSchemaEntry::decode(1, &extra).is_err());
        let mut values = parse_record(&entry().record()).unwrap();
        values[4] = SqliteValue::Null;
        assert!(NativeSchemaEntry::decode(1, &serialize_record(&values)).is_err());
    }

    #[test]
    fn create_text_binds_quoted_names_and_without_rowid_root_kind() {
        let table = Definition::parse("CREATE TABLE \"Odd Name\"(k TEXT PRIMARY KEY) WITHOUT ROWID").unwrap();
        assert_eq!(table.name, "Odd Name");
        assert!(!table.table_btree);
        let index = Definition::parse("CREATE INDEX main.\"By Value\" ON \"Odd Name\"(k)").unwrap();
        assert_eq!(index.table_name, "Odd Name");
        assert_eq!(index.kind, NativeSchemaKind::Index);
        assert!(!index.table_btree);
    }

    #[test]
    fn catalog_refuses_other_schemas_and_unsupported_definition_profiles() {
        for sql in ["", "CREATE TABLE t(x); CREATE TABLE u(x)", "CREATE TEMP TABLE t(x)",
            "CREATE TABLE aux.t(x)", "CREATE TABLE t AS SELECT 1", "CREATE VIEW v AS SELECT 1",
            "CREATE TABLE sqlite_hidden(x)", "CREATE TABLE t("] {
            assert!(Definition::parse(sql).is_err(), "sql={sql:?}");
        }
        assert!(Definition::parse(&"x".repeat(MAX_NATIVE_SCHEMA_SQL_BYTES + 1)).is_err());
    }
}
