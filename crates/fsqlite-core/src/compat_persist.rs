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

use std::path::Path;

use fsqlite_btree::BtreeCursorOps;
use fsqlite_btree::cursor::TransactionPageIo;
use fsqlite_error::{FrankenError, Result};
use fsqlite_pager::{MvccPager, SimplePager, TransactionHandle, TransactionMode};
use fsqlite_types::cx::Cx;
use fsqlite_types::record::{parse_record, serialize_record};
use fsqlite_types::value::SqliteValue;
use fsqlite_types::{PageNumber, PageSize};
use fsqlite_vdbe::codegen::{ColumnInfo, TableSchema};
use fsqlite_vdbe::engine::MemDatabase;
use fsqlite_vfs::{UnixVfs, host_fs};

/// SQLite file header magic bytes (first 16 bytes).
const SQLITE_MAGIC: &[u8; 16] = b"SQLite format 3\0";

/// Default page size used for newly-created databases.
const DEFAULT_PAGE_SIZE: PageSize = PageSize::DEFAULT;

// ── Public API ──────────────────────────────────────────────────────────

/// State loaded from a real SQLite file.
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
pub fn is_sqlite_format(path: &Path) -> bool {
    let Ok(data) = host_fs::read(path) else {
        return false;
    };
    data.len() >= SQLITE_MAGIC.len() && data[..SQLITE_MAGIC.len()] == *SQLITE_MAGIC
}

/// Persist `schema` + `db` to a real SQLite-format database file at `path`.
///
/// Overwrites any existing file. The resulting file is readable by `sqlite3`.
///
/// # Errors
///
/// Returns an error on I/O failure or if the B-tree layer rejects an
/// insertion (e.g. duplicate rowid in sqlite_master).
#[allow(clippy::too_many_lines)]
pub fn persist_to_sqlite(
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

    let cx = Cx::new();
    let vfs = UnixVfs::new();
    let pager = SimplePager::open(vfs, path, DEFAULT_PAGE_SIZE)?;
    let mut txn = pager.begin(&cx, TransactionMode::Immediate)?;

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
        let root_page = txn.allocate_page(&cx)?;

        // Initialize the root page as an empty leaf table B-tree.
        init_leaf_table_page(&cx, &mut txn, root_page, ps)?;

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
                cursor.table_insert(&cx, rowid, &payload)?;
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
            cursor.table_insert(&cx, rid, &record)?;
        }
    }

    // Fix up the database header on page 1: update page_count,
    // change_counter, and schema_cookie so sqlite3 validates the file.
    {
        let mut hdr_page = txn.get_page(&cx, PageNumber::ONE)?.into_vec();

        // Compute actual page count: max page number written.
        let max_page = master_entries
            .iter()
            .map(|(_, rp, _)| *rp)
            .max()
            .unwrap_or(1);

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

        txn.write_page(&cx, PageNumber::ONE, &hdr_page)?;
    }

    txn.commit(&cx)?;
    Ok(())
}

/// Load a real SQLite-format database file into `MemDatabase` + schema.
///
/// Reads sqlite_master from page 1, then reads each table's B-tree to
/// populate the in-memory store.
///
/// # Errors
///
/// Returns an error if the file is not a valid SQLite database, or on
/// I/O / B-tree navigation failures.
#[allow(clippy::too_many_lines, clippy::similar_names)]
pub fn load_from_sqlite(path: &Path) -> Result<LoadedState> {
    let cx = Cx::new();
    let vfs = UnixVfs::new();
    let pager = SimplePager::open(vfs, path, DEFAULT_PAGE_SIZE)?;
    let mut txn = pager.begin(&cx, TransactionMode::ReadOnly)?;

    let ps = DEFAULT_PAGE_SIZE.as_usize();
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

        if cursor.first(&cx)? {
            loop {
                let payload = cursor.payload(&cx)?;
                if let Some(values) = parse_record(&payload) {
                    entries.push(values);
                }
                if !cursor.next(&cx)? {
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

        // Parse the CREATE TABLE to extract column info.
        let columns = parse_columns_from_create_sql(&create_sql);
        let num_columns = columns.len();

        // Use the REAL root page from sqlite_master (5A.4: bd-1soh).
        #[allow(clippy::cast_possible_truncation)]
        let real_root_page = root_page_num as i32;
        db.create_table_at(real_root_page, num_columns);

        schema.push(TableSchema {
            name,
            root_page: real_root_page,
            columns,
            indexes: Vec::new(),
        });

        // Read all rows from this table's B-tree.
        let file_root =
            PageNumber::new(u32::try_from(root_page_num).unwrap_or(1)).unwrap_or(PageNumber::ONE);

        let mut cursor = fsqlite_btree::BtCursor::new(
            TransactionPageIo::new(&mut txn),
            file_root,
            usable_size,
            true,
        );

        if let Some(mem_table) = db.tables.get_mut(&real_root_page) {
            if cursor.first(&cx)? {
                loop {
                    let rowid = cursor.rowid(&cx)?;
                    let payload = cursor.payload(&cx)?;
                    if let Some(values) = parse_record(&payload) {
                        mem_table.insert_row(rowid, values);
                    }
                    if !cursor.next(&cx)? {
                        break;
                    }
                }
            }
        }
    }

    // Read schema_cookie and change_counter from the database header (page 1).
    let (schema_cookie, change_counter) = {
        let header_buf = txn.get_page(&cx, PageNumber::ONE)?;
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
    }
    sql.push(')');
    sql
}

/// Map affinity character to SQL type keyword.
const fn affinity_char_to_type(affinity: char) -> &'static str {
    match affinity {
        'd' | 'D' => "INTEGER",
        'e' | 'E' => "REAL",
        'B' => "BLOB",
        'A' => "NUMERIC",
        // 'C' and everything else → TEXT
        _ => "TEXT",
    }
}

/// Parse column definitions from a CREATE TABLE statement.
///
/// This is a best-effort parser that handles the common case of
/// `CREATE TABLE "name" ("col1" TYPE, "col2" TYPE, ...)`.
/// Parse column info from a CREATE TABLE SQL string.
///
/// Extracts column names and affinities from the column definitions.
/// Used by `load_from_sqlite` and `reload_memdb_from_pager` (bd-1ene).
pub fn parse_columns_from_create_sql(sql: &str) -> Vec<ColumnInfo> {
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
    let mut columns = Vec::new();

    for col_def in body.split(',') {
        let trimmed = col_def.trim();
        if trimmed.is_empty() {
            continue;
        }
        // Split into tokens; first token is column name (possibly quoted).
        let tokens: Vec<&str> = trimmed.split_whitespace().collect();
        if tokens.is_empty() {
            continue;
        }

        let name = tokens[0].trim_matches('"').to_owned();
        let type_str = tokens.get(1).copied().unwrap_or("TEXT");
        let affinity = type_to_affinity(type_str);
        let upper = trimmed.to_ascii_uppercase();
        let is_ipk = upper.contains("PRIMARY KEY") && type_str.to_ascii_uppercase().contains("INT");

        columns.push(ColumnInfo {
            name,
            affinity,
            is_ipk,
        });
    }

    columns
}

/// Map a SQL type keyword to an affinity character.
fn type_to_affinity(type_str: &str) -> char {
    let upper = type_str.to_uppercase();
    if upper.contains("INT") {
        'd'
    } else if upper.contains("REAL") || upper.contains("FLOAT") || upper.contains("DOUB") {
        'e'
    } else if upper.contains("BLOB") || upper.is_empty() {
        'B'
    } else if upper.contains("TEXT") || upper.contains("CHAR") || upper.contains("CLOB") {
        'C'
    } else {
        'A' // NUMERIC
    }
}

// ── Tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

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
                },
                ColumnInfo {
                    name: "name".to_owned(),
                    affinity: 'C',
                    is_ipk: false,
                },
            ],
            indexes: Vec::new(),
        }];

        (schema, db)
    }

    #[test]
    fn test_roundtrip_persist_and_load() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        let (schema, db) = make_test_schema_and_db();
        persist_to_sqlite(&db_path, &schema, &db, 0, 0).unwrap();

        assert!(db_path.exists(), "db file should exist");
        assert!(is_sqlite_format(&db_path), "should have SQLite magic");

        let loaded = load_from_sqlite(&db_path).unwrap();
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
        persist_to_sqlite(&db_path, &schema, &db, 0, 0).unwrap();

        assert!(is_sqlite_format(&db_path));

        let loaded = load_from_sqlite(&db_path).unwrap();
        assert!(loaded.schema.is_empty());
    }

    #[test]
    fn test_persist_creates_sqlite3_readable_file() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("readable.db");

        let (schema, db) = make_test_schema_and_db();
        persist_to_sqlite(&db_path, &schema, &db, 0, 0).unwrap();

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
        let loaded = load_from_sqlite(&db_path).unwrap();
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
                }],
                indexes: Vec::new(),
            },
            TableSchema {
                name: "beta".to_owned(),
                root_page: root_b,
                columns: vec![ColumnInfo {
                    name: "num".to_owned(),
                    affinity: 'd',
                    is_ipk: false,
                }],
                indexes: Vec::new(),
            },
        ];

        persist_to_sqlite(&db_path, &schema, &db, 0, 0).unwrap();
        let loaded = load_from_sqlite(&db_path).unwrap();

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
        assert_eq!(cols[0].affinity, 'd');
        assert_eq!(cols[1].name, "name");
        assert_eq!(cols[1].affinity, 'C');
        assert_eq!(cols[2].name, "data");
        assert_eq!(cols[2].affinity, 'B');
    }

    #[test]
    fn test_type_to_affinity_mapping() {
        assert_eq!(type_to_affinity("INTEGER"), 'd');
        assert_eq!(type_to_affinity("INT"), 'd');
        assert_eq!(type_to_affinity("REAL"), 'e');
        assert_eq!(type_to_affinity("FLOAT"), 'e');
        assert_eq!(type_to_affinity("TEXT"), 'C');
        assert_eq!(type_to_affinity("VARCHAR"), 'C');
        assert_eq!(type_to_affinity("BLOB"), 'B');
        assert_eq!(type_to_affinity("NUMERIC"), 'A');
    }

    #[test]
    fn test_overwrite_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("overwrite.db");

        // Write once.
        let (schema, db) = make_test_schema_and_db();
        persist_to_sqlite(&db_path, &schema, &db, 0, 0).unwrap();

        // Overwrite with empty.
        persist_to_sqlite(&db_path, &[], &MemDatabase::new(), 0, 0).unwrap();

        let loaded = load_from_sqlite(&db_path).unwrap();
        assert!(loaded.schema.is_empty());
    }
}
