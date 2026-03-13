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

#[cfg(not(target_arch = "wasm32"))]
use std::path::Path;

use fsqlite_ast::{SortDirection, Statement};
#[cfg(not(target_arch = "wasm32"))]
use fsqlite_btree::BtreeCursorOps;
#[cfg(not(target_arch = "wasm32"))]
use fsqlite_btree::cursor::TransactionPageIo;
use fsqlite_error::{FrankenError, Result};
#[cfg(not(target_arch = "wasm32"))]
use fsqlite_pager::{MvccPager, SimplePager, TransactionHandle, TransactionMode};
use fsqlite_parser::Parser;
use fsqlite_types::StrictColumnType;
#[cfg(not(target_arch = "wasm32"))]
use fsqlite_types::cx::Cx;
#[cfg(not(target_arch = "wasm32"))]
use fsqlite_types::record::{parse_record, serialize_record};
#[cfg(not(target_arch = "wasm32"))]
use fsqlite_types::value::SqliteValue;
#[cfg(not(target_arch = "wasm32"))]
use fsqlite_types::{PageNumber, PageSize};
use fsqlite_vdbe::codegen::{ColumnInfo, TableSchema};
use fsqlite_vdbe::engine::MemDatabase;
#[cfg(all(not(target_arch = "wasm32"), unix))]
use fsqlite_vfs::UnixVfs as PlatformVfs;
#[cfg(all(not(target_arch = "wasm32"), target_os = "windows"))]
use fsqlite_vfs::WindowsVfs as PlatformVfs;
#[cfg(not(target_arch = "wasm32"))]
use fsqlite_vfs::host_fs;

/// SQLite file header magic bytes (first 16 bytes).
#[cfg(not(target_arch = "wasm32"))]
const SQLITE_MAGIC: &[u8; 16] = b"SQLite format 3\0";

/// Default page size used for newly-created databases.
#[cfg(not(target_arch = "wasm32"))]
const DEFAULT_PAGE_SIZE: PageSize = PageSize::DEFAULT;

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
#[cfg(not(target_arch = "wasm32"))]
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
#[cfg(not(target_arch = "wasm32"))]
pub fn persist_to_sqlite(
    cx: &Cx,
    path: &Path,
    schema: &[TableSchema],
    db: &MemDatabase,
    schema_cookie: u32,
    change_counter: u32,
) -> Result<()> {
    // Remove existing file so the pager creates a fresh one.
    if path.exists() {
        // Truncate to empty so the pager treats it as a fresh DB, without
        // requiring delete permissions on the parent directory.
        host_fs::create_empty_file(path)?;
    }

    let vfs = PlatformVfs::new();
    let pager = SimplePager::open_with_cx(cx, vfs, path, DEFAULT_PAGE_SIZE)?;
    let mut txn = pager.begin(cx, TransactionMode::Immediate)?;

    let ps = DEFAULT_PAGE_SIZE.as_usize();
    let usable_size =
        u32::try_from(ps).map_err(|_| FrankenError::internal("page size exceeds u32"))?;

    // Track (name, root_page, create_sql) for sqlite_master entries.
    let mut master_entries: Vec<(String, u32, String)> = Vec::new();

    // Write each table's data into its own B-tree.
    for table in schema {
        let Some(mem_table) = db.get_table(table.root_page) else {
            continue;
        };

        // Allocate a fresh root page for this table in the on-disk file.
        let root_page = txn.allocate_page(cx)?;

        // Initialize the root page as an empty leaf table B-tree.
        init_leaf_table_page(cx, &mut txn, root_page, ps)?;

        // Insert all rows.
        {
            let mut cursor = fsqlite_btree::BtCursor::new(
                TransactionPageIo::new(&mut txn),
                root_page,
                usable_size,
                true,
            );
            for (rowid, values) in mem_table.iter_rows() {
                let payload = serialize_record(values);
                cursor.table_insert(cx, rowid, &payload)?;
            }
        }

        // Build CREATE TABLE SQL for sqlite_master.
        let create_sql = build_create_table_sql(table);
        master_entries.push((table.name.clone(), root_page.get(), create_sql));
    }

    // Write sqlite_master entries into page 1's B-tree.
    // sqlite_master columns: type TEXT, name TEXT, tbl_name TEXT, rootpage INTEGER, sql TEXT
    {
        let master_root = PageNumber::ONE;
        let mut cursor = fsqlite_btree::BtCursor::new(
            TransactionPageIo::new(&mut txn),
            master_root,
            usable_size,
            true,
        );

        for (rowid, (name, root_page_num, create_sql)) in master_entries.iter().enumerate() {
            let record = serialize_record(&[
                SqliteValue::Text("table".to_owned()),
                SqliteValue::Text(name.clone()),
                SqliteValue::Text(name.clone()),
                SqliteValue::Integer(i64::from(*root_page_num)),
                SqliteValue::Text(create_sql.clone()),
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

        // Compute actual page count by finding the highest page number that was allocated.
        // In a fresh database with no freelist, allocating one more page gives us (max_page + 1).
        let next_page = txn.allocate_page(cx)?.get();
        let max_page = next_page.saturating_sub(1).max(1);

        // page_count at offset 28 (4 bytes, big-endian)
        hdr_page[28..32].copy_from_slice(&max_page.to_be_bytes());

        // change_counter at offset 24 — tracked by Connection, must be
        // non-zero for sqlite3 to trust the header.  Use at least 1.
        let effective_counter = change_counter.max(1);
        hdr_page[24..28].copy_from_slice(&effective_counter.to_be_bytes());

        // schema_cookie at offset 40 — tracked by Connection, incremented
        // on every DDL operation.  Non-zero so sqlite3 re-reads schema.
        let effective_cookie = schema_cookie.max(1);
        hdr_page[40..44].copy_from_slice(&effective_cookie.to_be_bytes());

        // version-valid-for at offset 92 (must match change_counter)
        hdr_page[92..96].copy_from_slice(&effective_counter.to_be_bytes());

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
#[cfg(not(target_arch = "wasm32"))]
pub fn load_from_sqlite(cx: &Cx, path: &Path) -> Result<LoadedState> {
    let vfs = PlatformVfs::new();
    let pager = SimplePager::open_with_cx(cx, vfs, path, DEFAULT_PAGE_SIZE)?;
    let mut txn = pager.begin(cx, TransactionMode::ReadOnly)?;

    let ps = pager.page_size().as_usize();
    let usable_size =
        u32::try_from(ps).map_err(|_| FrankenError::internal("page size exceeds u32"))?;

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

        if cursor.first(cx)? {
            loop {
                let rowid = cursor.rowid(cx)?;
                let payload = cursor.payload(cx)?;
                let values =
                    parse_record(&payload).ok_or_else(|| FrankenError::DatabaseCorrupt {
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
    let mut schema = Vec::new();
    let mut db = MemDatabase::new();

    for entry in &master_entries {
        if entry.len() < 5 {
            continue;
        }
        let entry_type = match &entry[0] {
            SqliteValue::Text(s) => s.as_str(),
            _ => continue,
        };
        if entry_type != "table" {
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

        // Stock SQLite virtual tables (FTS5, rtree, etc.) have
        // rootpage=0 and CREATE SQL beginning with CREATE VIRTUAL TABLE.
        // Skip only that combination: FrankenSQLite may preserve virtual-table
        // SQL text while still materializing the table onto a real root page.
        let is_virtual = root_page_num == 0
            && create_sql
                .trim_start()
                .to_ascii_uppercase()
                .starts_with("CREATE VIRTUAL TABLE");
        if is_virtual {
            continue;
        }
        let root_page_u32 = validate_sqlite_master_root_page(&name, root_page_num)?;

        // Parse the CREATE TABLE to extract column info.
        let columns = parse_columns_from_sqlite_master_sql(&create_sql);
        let num_columns = columns.len();

        // Use the REAL root page from sqlite_master (5A.4: bd-1soh).
        let real_root_page =
            i32::try_from(root_page_u32).expect("validated root page must fit MemDatabase");
        db.create_table_at(real_root_page, num_columns);

        let table_name_for_err = name.clone();
        schema.push(TableSchema {
            name,
            root_page: real_root_page,
            columns,
            indexes: Vec::new(),
            strict: is_strict_table_sql(&create_sql),
            foreign_keys: Vec::new(),
            check_constraints: Vec::new(),
        });

        // Read all rows from this table's B-tree.
        let file_root =
            PageNumber::new(root_page_u32).expect("validated sqlite_master root page is positive");

        let mut cursor = fsqlite_btree::BtCursor::new(
            TransactionPageIo::new(&mut txn),
            file_root,
            usable_size,
            true,
        );

        if let Some(mem_table) = db.tables.get_mut(&real_root_page) {
            if cursor.first(cx)? {
                loop {
                    let rowid = cursor.rowid(cx)?;
                    let payload = cursor.payload(cx)?;
                    let values = parse_record(&payload).ok_or_else(|| {
                        FrankenError::DatabaseCorrupt {
                            detail: format!(
                                "table `{table_name_for_err}` rowid {rowid} payload is not a valid SQLite record"
                            ),
                        }
                    })?;
                    mem_table.insert_row(rowid, values);
                    if !cursor.next(cx)? {
                        break;
                    }
                }
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
#[cfg(not(target_arch = "wasm32"))]
fn init_leaf_table_page(
    cx: &Cx,
    txn: &mut impl TransactionHandle,
    page_no: PageNumber,
    page_size: usize,
) -> Result<()> {
    let mut page = vec![0u8; page_size];
    page[0] = 0x0D; // Leaf table
    // cell_count = 0 (bytes 3..5)
    page[3..5].copy_from_slice(&0u16.to_be_bytes());
    // cell content area starts at end of page
    #[allow(clippy::cast_possible_truncation)]
    let content_start = page_size as u16;
    page[5..7].copy_from_slice(&content_start.to_be_bytes());
    txn.write_page(cx, page_no, &page)
}

/// Reconstruct a `CREATE TABLE` statement from a `TableSchema`.
pub(crate) fn build_create_table_sql(table: &TableSchema) -> String {
    use std::fmt::Write as _;
    let mut sql = format!("CREATE TABLE \"{}\" (", table.name);
    for (i, col) in table.columns.iter().enumerate() {
        if i > 0 {
            sql.push_str(", ");
        }
        let type_kw = affinity_char_to_type(col.affinity);
        let _ = write!(sql, "\"{}\" {type_kw}", col.name);
        if col.is_ipk {
            sql.push_str(" PRIMARY KEY");
        }
        if col.notnull && !col.is_ipk {
            sql.push_str(" NOT NULL");
        }
        if col.unique && !col.is_ipk {
            sql.push_str(" UNIQUE");
        }
        if let Some(ref default) = col.default_value {
            sql.push_str(" DEFAULT ");
            sql.push_str(default);
        }
        if let Some(ref collation) = col.collation {
            sql.push_str(" COLLATE ");
            sql.push_str(collation);
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
    sql.push(')');
    if table.strict {
        sql.push_str(" STRICT");
    }
    sql
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
            "CREATE UNIQUE INDEX \"{}\" ON \"{}\" (",
            index_name, table_name
        )
    } else {
        format!("CREATE INDEX \"{}\" ON \"{}\" (", index_name, table_name)
    };
    for (i, term) in terms.iter().enumerate() {
        if i > 0 {
            sql.push_str(", ");
        }
        let _ = write!(sql, "\"{}\"", term.column_name);
        if let Some(collation) = term.collation {
            let _ = write!(sql, " COLLATE \"{}\"", collation);
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

/// Map affinity character to SQL type keyword.
const fn affinity_char_to_type(affinity: char) -> &'static str {
    match affinity {
        'd' | 'D' => "INTEGER",
        'e' | 'E' => "REAL",
        'a' | 'A' => "BLOB",
        'c' | 'C' => "NUMERIC",
        // 'b'/'B' (TEXT affinity) and all unknowns default to TEXT.
        _ => "TEXT",
    }
}

/// Parse column info from a CREATE TABLE SQL string.
///
/// This is a best-effort parser that handles the common case of
/// `CREATE TABLE "name" ("col1" TYPE, "col2" TYPE, ...)`.
/// Extracts column names and affinities from the column definitions.
/// Used by `load_from_sqlite` and `reload_memdb_from_pager` (bd-1ene).
pub fn parse_columns_from_create_sql(sql: &str) -> Vec<ColumnInfo> {
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
            let upper = col_def.to_ascii_uppercase();
            let is_ipk = !is_without_rowid
                && upper.contains("PRIMARY KEY")
                && !upper.contains("PRIMARY KEY DESC")
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

            // Extract COLLATE name from column definition.
            let collation = upper
                .find("COLLATE ")
                .map(|pos| {
                    // Read the collation name from the original (non-uppercased) text.
                    let after = &col_def[pos + 8..];
                    after
                        .split_whitespace()
                        .next()
                        .unwrap_or("")
                        .trim_end_matches(',')
                        .to_owned()
                })
                .filter(|s| !s.is_empty());

            Some(ColumnInfo {
                name,
                affinity,
                is_ipk,
                type_name,
                notnull: upper.contains("NOT NULL"),
                unique: upper.contains("UNIQUE") || upper.contains("PRIMARY KEY"),
                default_value,
                strict_type,
                generated_expr: None,
                generated_stored: None,
                collation,
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
            detail: format!("table `{name}` has invalid rootpage {root_page_num} in sqlite_master"),
        });
    }

    let root_page_u32 =
        u32::try_from(root_page_num).map_err(|_| FrankenError::DatabaseCorrupt {
            detail: format!(
                "table `{name}` has out-of-range rootpage {root_page_num} in sqlite_master"
            ),
        })?;
    i32::try_from(root_page_u32).map_err(|_| FrankenError::DatabaseCorrupt {
        detail: format!("table `{name}` has rootpage {root_page_num} that exceeds supported range"),
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
    let Some(close_paren) = sql.rfind(')') else {
        return false;
    };
    let tail = &sql[close_paren + 1..];
    let mut tokens = Vec::new();
    let mut token = String::new();
    for ch in tail.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            token.push(ch.to_ascii_uppercase());
        } else if !token.is_empty() {
            tokens.push(std::mem::take(&mut token));
        }
    }
    if !token.is_empty() {
        tokens.push(token);
    }
    tokens
        .windows(2)
        .any(|window| window[0] == "WITHOUT" && window[1] == "ROWID")
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
        });
    }

    columns
}

/// Return true when CREATE TABLE SQL declares the table as STRICT.
#[must_use]
pub fn is_strict_table_sql(sql: &str) -> bool {
    let Some(close_paren) = sql.rfind(')') else {
        return false;
    };
    let tail = &sql[close_paren + 1..];
    let mut token = String::new();
    for ch in tail.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            token.push(ch.to_ascii_uppercase());
        } else if !token.is_empty() {
            if token == "STRICT" {
                return true;
            }
            token.clear();
        }
    }
    token == "STRICT"
}

/// Return true when CREATE TABLE SQL declares AUTOINCREMENT.
#[must_use]
pub fn is_autoincrement_table_sql(sql: &str) -> bool {
    let mut token = String::new();
    for ch in sql.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            token.push(ch.to_ascii_uppercase());
        } else if !token.is_empty() {
            if token == "AUTOINCREMENT" {
                return true;
            }
            token.clear();
        }
    }
    token == "AUTOINCREMENT"
}

/// Extract CHECK constraint expressions from a CREATE TABLE SQL string.
///
/// Finds `CHECK(...)` clauses in the column-def body and returns the
/// expression text (inside the parentheses) for each one.
#[must_use]
pub fn extract_check_constraints_from_sql(sql: &str) -> Vec<String> {
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
            let end = trimmed.find(char::is_whitespace).unwrap_or(trimmed.len());
            (&trimmed[..end], &trimmed[end..])
        }
    };
    Some((strip_identifier_quotes(name_raw), remainder.trim_start()))
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
/// and top-level `-- ...` line comments.
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
/// `DEFAULT 'string'`, and `DEFAULT (expr)`.
fn extract_default_value(remainder: &str) -> Option<String> {
    let upper = remainder.to_ascii_uppercase();
    let pos = upper.find("DEFAULT")?;
    let after = remainder[pos + 7..].trim_start();
    if after.is_empty() {
        return None;
    }
    // Parenthesized expression: DEFAULT (...)
    if after.starts_with('(') {
        let mut depth = 0i32;
        for (i, ch) in after.char_indices() {
            if ch == '(' {
                depth += 1;
            } else if ch == ')' {
                depth -= 1;
                if depth == 0 {
                    return Some(after[..=i].to_owned());
                }
            }
        }
        return None;
    }
    // Quoted string: DEFAULT '...'
    if let Some(rest) = after.strip_prefix('\'') {
        let mut i = 0;
        let bytes = rest.as_bytes();
        while i < bytes.len() {
            if bytes[i] == b'\'' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
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

    fn make_test_schema_and_db() -> (Vec<TableSchema>, MemDatabase) {
        let mut db = MemDatabase::new();
        let root = db.create_table(2);
        let table = db.tables.get_mut(&root).unwrap();
        table.insert_row(
            1,
            vec![
                SqliteValue::Integer(42),
                SqliteValue::Text("hello".to_owned()),
            ],
        );
        table.insert_row(
            2,
            vec![
                SqliteValue::Integer(99),
                SqliteValue::Text("world".to_owned()),
            ],
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
                },
            ],
            indexes: Vec::new(),
            strict: false,
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
        assert_eq!(rows[0].1[1], SqliteValue::Text("hello".to_owned()));
        assert_eq!(rows[1].0, 2);
        assert_eq!(rows[1].1[0], SqliteValue::Integer(99));
        assert_eq!(rows[1].1[1], SqliteValue::Text("world".to_owned()));
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
        assert_eq!(rows[0].1[1], SqliteValue::Text("alpha".to_owned()));
        assert_eq!(rows[1].1[0], SqliteValue::Integer(20));
        assert_eq!(rows[1].1[1], SqliteValue::Text("beta".to_owned()));
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
            .insert_row(1, vec![SqliteValue::Text("row_a".to_owned())]);

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
                }],
                indexes: Vec::new(),
                strict: false,
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
                }],
                indexes: Vec::new(),
                strict: false,
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
        assert_eq!(rows_a[0].1[0], SqliteValue::Text("row_a".to_owned()));

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
            }],
            indexes: Vec::new(),
            strict: true,
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
        let table = loaded
            .schema
            .iter()
            .find(|table| table.name.eq_ignore_ascii_case("docs"))
            .expect("materialized virtual table should survive direct load");
        let column_names: Vec<&str> = table
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect();
        assert_eq!(column_names, vec!["subject", "body"]);
        let mem_table = loaded
            .db
            .get_table(table.root_page)
            .expect("loaded table should exist in MemDatabase");
        let rows: Vec<_> = mem_table.iter_rows().collect();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0, 1);
        assert_eq!(rows[0].1[0], SqliteValue::Text("Hello".to_owned()));
        assert_eq!(rows[0].1[1], SqliteValue::Text("Rust world".to_owned()));
        assert_eq!(rows[1].0, 2);
        assert_eq!(rows[1].1[0], SqliteValue::Text("Other".to_owned()));
        assert_eq!(rows[1].1[1], SqliteValue::Text("Nothing".to_owned()));
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
