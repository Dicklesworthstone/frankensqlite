//! Deterministic unit-test fixture corpus and seed harness (bd-mblr.6.4).
//!
//! Provides reusable, deterministic fixtures for storage pages, SQL statements,
//! and execution traces so that failures are exactly reproducible. Every fixture
//! is derived from a single root seed using the project's standard hash-based
//! derivation (`xxh3_64`), guaranteeing cross-platform determinism.
//!
//! # Seed Contract
//!
//! All fixtures derive from [`FIXTURE_SEED_BASE`] via
//! [`FixtureSeed::derive`]. The derivation is:
//!
//! ```text
//! H(base_seed_bytes || domain_tag || scope_id)
//! ```
//!
//! where `H = xxh3_64`. This means:
//! - Same inputs always produce the same seed (determinism).
//! - Different domain tags or scope IDs always produce different seeds (isolation).
//! - The seed is 64-bit, suitable for seeding any PRNG or fixture generator.
//!
//! # Fixture Categories
//!
//! | Category | Domain tag | What it covers |
//! |----------|-----------|----------------|
//! | Page     | `"page"`  | B-tree leaf/interior pages, WAL frames, overflow |
//! | SQL      | `"sql"`   | Statements by taxonomy family (SQL, TXN, FUN, …) |
//! | Trace    | `"trace"` | VDBE bytecode sequences, cursor navigation |
//! | Value    | `"value"` | `SqliteValue` instances covering all storage classes |
//!
//! # Usage
//!
//! ```rust
//! use fsqlite_harness::unit_fixtures::{FixtureSeed, FixtureCatalog};
//!
//! let catalog = FixtureCatalog::build();
//! assert!(!catalog.sql_fixtures.is_empty());
//! assert!(!catalog.page_fixtures.is_empty());
//!
//! // Deterministic seed for a specific test
//! let seed = FixtureSeed::derive("my_test_case");
//! assert_eq!(seed, FixtureSeed::derive("my_test_case")); // always same
//! ```

use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};
use xxhash_rust::xxh3::xxh3_64;

use crate::corpus_ingest::Family;

/// Bead identifier for log correlation.
const BEAD_ID: &str = "bd-mblr.6.4";

// ─── Seed Contract ──────────────────────────────────────────────────────

/// Root seed for the unit fixture corpus.
///
/// Hex-encodes `"FIXTURE"` (0x46_49_58_54_55_52_45 padded to 8 bytes).
pub const FIXTURE_SEED_BASE: u64 = 0x4649_5854_5552_4500;

/// Domain tags for seed derivation — each fixture category gets its own
/// namespace so seeds never collide across categories.
pub mod domain {
    /// Storage page fixtures.
    pub const PAGE: &[u8] = b"page";
    /// SQL statement fixtures.
    pub const SQL: &[u8] = b"sql";
    /// VDBE execution trace fixtures.
    pub const TRACE: &[u8] = b"trace";
    /// `SqliteValue` fixtures.
    pub const VALUE: &[u8] = b"value";
}

/// A deterministically derived seed for a specific fixture scope.
///
/// The derivation is `xxh3_64(base_seed_bytes || domain_tag || scope_id)`.
/// Two calls with the same arguments always return the same value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FixtureSeed(pub u64);

impl FixtureSeed {
    /// Derive a fixture seed from the default base using the "sql" domain.
    ///
    /// This is the most common entry point for test code.
    #[must_use]
    pub fn derive(scope_id: &str) -> Self {
        Self::derive_with(FIXTURE_SEED_BASE, domain::SQL, scope_id)
    }

    /// Derive a fixture seed with explicit base, domain, and scope.
    #[must_use]
    pub fn derive_with(base_seed: u64, domain_tag: &[u8], scope_id: &str) -> Self {
        let mut buf = Vec::with_capacity(8 + domain_tag.len() + scope_id.len());
        buf.extend_from_slice(&base_seed.to_le_bytes());
        buf.extend_from_slice(domain_tag);
        buf.extend_from_slice(scope_id.as_bytes());
        Self(xxh3_64(&buf))
    }

    /// Derive a child seed from this one, useful for generating multiple
    /// related fixtures within the same scope.
    #[must_use]
    pub fn child(self, index: u32) -> Self {
        let mut buf = [0u8; 12];
        buf[..8].copy_from_slice(&self.0.to_le_bytes());
        buf[8..12].copy_from_slice(&index.to_le_bytes());
        Self(xxh3_64(&buf))
    }

    /// Raw seed value.
    #[must_use]
    pub const fn raw(self) -> u64 {
        self.0
    }
}

impl fmt::Display for FixtureSeed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "seed:0x{:016X}", self.0)
    }
}

// ─── Page Fixtures ──────────────────────────────────────────────────────

/// B-tree page type flags (matches the SQLite file format).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PageType {
    /// Interior table B-tree page (flag byte 0x05).
    InteriorTable,
    /// Leaf table B-tree page (flag byte 0x0D).
    LeafTable,
    /// Interior index B-tree page (flag byte 0x02).
    InteriorIndex,
    /// Leaf index B-tree page (flag byte 0x0A).
    LeafIndex,
}

impl PageType {
    /// The flag byte value stored at offset 0 of the page header.
    #[must_use]
    pub const fn flag_byte(self) -> u8 {
        match self {
            Self::InteriorTable => 0x05,
            Self::LeafTable => 0x0D,
            Self::InteriorIndex => 0x02,
            Self::LeafIndex => 0x0A,
        }
    }

    /// All page types in canonical order.
    pub const ALL: [Self; 4] = [
        Self::InteriorTable,
        Self::LeafTable,
        Self::InteriorIndex,
        Self::LeafIndex,
    ];
}

impl fmt::Display for PageType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InteriorTable => f.write_str("interior-table"),
            Self::LeafTable => f.write_str("leaf-table"),
            Self::InteriorIndex => f.write_str("interior-index"),
            Self::LeafIndex => f.write_str("leaf-index"),
        }
    }
}

/// A deterministic page fixture with known content and metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PageFixture {
    /// Human-readable identifier for this fixture.
    pub id: String,
    /// The page type.
    pub page_type: PageType,
    /// Page number (1-based).
    pub page_number: u32,
    /// Page size in bytes.
    pub page_size: u32,
    /// Number of cells on this page.
    pub cell_count: u16,
    /// The seed used to generate this fixture's content.
    pub seed: FixtureSeed,
    /// Raw page data (deterministic, reproducible).
    pub data: Vec<u8>,
    /// Description of what this fixture tests.
    pub description: String,
}

/// Build the canonical set of page fixtures.
///
/// Each fixture uses a deterministic seed derived from `FIXTURE_SEED_BASE`
/// with the `"page"` domain tag, ensuring identical output on every call.
#[must_use]
#[allow(clippy::too_many_lines, clippy::vec_init_then_push)]
pub fn build_page_fixtures() -> Vec<PageFixture> {
    let mut fixtures = Vec::new();

    // Empty leaf table page (simplest valid B-tree page).
    let seed = FixtureSeed::derive_with(FIXTURE_SEED_BASE, domain::PAGE, "empty_leaf_table");
    fixtures.push(PageFixture {
        id: "page-empty-leaf-table".to_owned(),
        page_type: PageType::LeafTable,
        page_number: 2,
        page_size: 4096,
        cell_count: 0,
        seed,
        data: build_empty_page(PageType::LeafTable, 4096),
        description: "Empty leaf table page — zero cells, valid header".to_owned(),
    });

    // Leaf table page with a single cell.
    let seed = FixtureSeed::derive_with(FIXTURE_SEED_BASE, domain::PAGE, "single_cell_leaf");
    fixtures.push(PageFixture {
        id: "page-single-cell-leaf".to_owned(),
        page_type: PageType::LeafTable,
        page_number: 3,
        page_size: 4096,
        cell_count: 1,
        seed,
        data: build_single_cell_leaf_page(4096, seed),
        description: "Leaf table page with one cell (rowid=1, INTEGER column)".to_owned(),
    });

    // Interior table page with two children.
    let seed = FixtureSeed::derive_with(FIXTURE_SEED_BASE, domain::PAGE, "interior_table_2ch");
    fixtures.push(PageFixture {
        id: "page-interior-table-2ch".to_owned(),
        page_type: PageType::InteriorTable,
        page_number: 4,
        page_size: 4096,
        cell_count: 1,
        seed,
        data: build_interior_table_page(4096, 2, 3),
        description: "Interior table page with one divider (left child=2, right child=3)"
            .to_owned(),
    });

    // Empty leaf index page.
    let seed = FixtureSeed::derive_with(FIXTURE_SEED_BASE, domain::PAGE, "empty_leaf_index");
    fixtures.push(PageFixture {
        id: "page-empty-leaf-index".to_owned(),
        page_type: PageType::LeafIndex,
        page_number: 5,
        page_size: 4096,
        cell_count: 0,
        seed,
        data: build_empty_page(PageType::LeafIndex, 4096),
        description: "Empty leaf index page — zero cells, valid header".to_owned(),
    });

    // Leaf table page with multiple cells (stress cell pointer array).
    let seed = FixtureSeed::derive_with(FIXTURE_SEED_BASE, domain::PAGE, "multi_cell_leaf");
    let cell_count = 5;
    fixtures.push(PageFixture {
        id: "page-multi-cell-leaf".to_owned(),
        page_type: PageType::LeafTable,
        page_number: 6,
        page_size: 4096,
        cell_count,
        seed,
        data: build_multi_cell_leaf_page(4096, cell_count, seed),
        description: format!(
            "Leaf table page with {cell_count} cells (rowids 1..{cell_count}, mixed types)"
        ),
    });

    // Empty interior index page.
    let seed = FixtureSeed::derive_with(FIXTURE_SEED_BASE, domain::PAGE, "empty_interior_index");
    fixtures.push(PageFixture {
        id: "page-empty-interior-index".to_owned(),
        page_type: PageType::InteriorIndex,
        page_number: 9,
        page_size: 4096,
        cell_count: 0,
        seed,
        data: build_empty_page(PageType::InteriorIndex, 4096),
        description: "Empty interior index page — zero cells, valid header".to_owned(),
    });

    // Small page size (512 bytes) — tests boundary conditions.
    let seed = FixtureSeed::derive_with(FIXTURE_SEED_BASE, domain::PAGE, "small_page_leaf");
    fixtures.push(PageFixture {
        id: "page-small-512-leaf".to_owned(),
        page_type: PageType::LeafTable,
        page_number: 7,
        page_size: 512,
        cell_count: 0,
        seed,
        data: build_empty_page(PageType::LeafTable, 512),
        description: "Empty leaf table on 512-byte page — minimum page size boundary".to_owned(),
    });

    // Large page size (65536 bytes) — maximum page size boundary.
    let seed = FixtureSeed::derive_with(FIXTURE_SEED_BASE, domain::PAGE, "large_page_leaf");
    fixtures.push(PageFixture {
        id: "page-large-65536-leaf".to_owned(),
        page_type: PageType::LeafTable,
        page_number: 8,
        page_size: 65_536,
        cell_count: 0,
        seed,
        data: build_empty_page(PageType::LeafTable, 65_536),
        description: "Empty leaf table on 65536-byte page — maximum page size boundary".to_owned(),
    });

    fixtures
}

/// Build an empty B-tree page with a valid header.
#[allow(clippy::cast_possible_truncation)]
fn build_empty_page(page_type: PageType, page_size: u32) -> Vec<u8> {
    let size = page_size as usize;
    let mut page = vec![0u8; size];

    // Header layout for leaf pages: 8 bytes.
    // Header layout for interior pages: 12 bytes.
    let header_size: u16 = if matches!(page_type, PageType::InteriorTable | PageType::InteriorIndex)
    {
        12
    } else {
        8
    };

    // Byte 0: page type flag.
    page[0] = page_type.flag_byte();

    // Bytes 1-2: offset to first freeblock (0 = none).
    page[1] = 0;
    page[2] = 0;

    // Bytes 3-4: number of cells (0).
    page[3] = 0;
    page[4] = 0;

    // Bytes 5-6: offset to first byte of cell content area.
    // For an empty page, this points to the end of the page.
    let content_offset = if page_size == 65_536 {
        0u16
    } else {
        page_size as u16
    };
    page[5] = (content_offset >> 8) as u8;
    page[6] = (content_offset & 0xFF) as u8;

    // Byte 7: number of fragmented free bytes (0).
    page[7] = 0;

    // For interior pages, bytes 8-11: right-most child page pointer.
    if header_size == 12 {
        // Set right-most pointer to 0 (placeholder).
        page[8] = 0;
        page[9] = 0;
        page[10] = 0;
        page[11] = 0;
    }

    page
}

/// Build a leaf table page with a single cell containing (rowid=1, value=42).
#[allow(clippy::cast_possible_truncation)]
fn build_single_cell_leaf_page(page_size: u32, _seed: FixtureSeed) -> Vec<u8> {
    let size = page_size as usize;
    let mut page = vec![0u8; size];
    let header_size: u16 = 8;

    // Cell content: payload_size(varint) || rowid(varint) || record
    // Record: header_size(varint=2) || serial_type(varint=1 → 8-bit int) || value(1 byte)
    let cell_content: Vec<u8> = vec![
        4,  // payload size = 4 bytes
        1,  // rowid = 1
        2,  // record header size = 2
        1,  // serial type 1 = 8-bit signed integer
        42, // value = 42
    ];
    let cell_len = cell_content.len();
    let cell_offset = size - cell_len;

    // Page header.
    page[0] = PageType::LeafTable.flag_byte();
    page[1] = 0; // first freeblock hi
    page[2] = 0; // first freeblock lo
    page[3] = 0; // cell count hi
    page[4] = 1; // cell count lo (1 cell)
    let co = cell_offset as u16;
    page[5] = (co >> 8) as u8;
    page[6] = (co & 0xFF) as u8;
    page[7] = 0; // fragmented free bytes

    // Cell pointer array: 2 bytes, big-endian offset to cell content.
    let ptr_offset = header_size as usize;
    page[ptr_offset] = (co >> 8) as u8;
    page[ptr_offset + 1] = (co & 0xFF) as u8;

    // Write cell content at end of page.
    page[cell_offset..cell_offset + cell_len].copy_from_slice(&cell_content);

    page
}

/// Build an interior table page with one divider cell.
#[allow(clippy::cast_possible_truncation)]
fn build_interior_table_page(page_size: u32, left_child: u32, right_child: u32) -> Vec<u8> {
    let size = page_size as usize;
    let mut page = vec![0u8; size];
    let header_size: u16 = 12;

    // Interior table cell: left_child_ptr(4 bytes) || rowid(varint)
    // The divider key is the maximum rowid in the left subtree.
    let cell_content: Vec<u8> = vec![
        (left_child >> 24) as u8,
        (left_child >> 16) as u8,
        (left_child >> 8) as u8,
        left_child as u8,
        10, // divider rowid = 10
    ];
    let cell_len = cell_content.len();
    let cell_offset = size - cell_len;

    // Page header.
    page[0] = PageType::InteriorTable.flag_byte();
    page[1] = 0;
    page[2] = 0;
    page[3] = 0;
    page[4] = 1; // 1 cell
    let co = cell_offset as u16;
    page[5] = (co >> 8) as u8;
    page[6] = (co & 0xFF) as u8;
    page[7] = 0;

    // Right-most child pointer (bytes 8-11).
    page[8] = (right_child >> 24) as u8;
    page[9] = (right_child >> 16) as u8;
    page[10] = (right_child >> 8) as u8;
    page[11] = right_child as u8;

    // Cell pointer array.
    let ptr_offset = header_size as usize;
    page[ptr_offset] = (co >> 8) as u8;
    page[ptr_offset + 1] = (co & 0xFF) as u8;

    // Write cell content.
    page[cell_offset..cell_offset + cell_len].copy_from_slice(&cell_content);

    page
}

/// Build a leaf table page with multiple cells (deterministic content).
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn build_multi_cell_leaf_page(page_size: u32, cell_count: u16, seed: FixtureSeed) -> Vec<u8> {
    let size = page_size as usize;
    let mut page = vec![0u8; size];
    let header_size: u16 = 8;

    // Build cells from the end of the page backward.
    let mut cell_offsets: Vec<u16> = Vec::new();
    let mut write_pos = size;

    for i in 0..cell_count {
        let child_seed = seed.child(u32::from(i));
        // Vary content deterministically based on seed.
        let val_byte = (child_seed.raw() & 0xFF) as u8;
        let rowid = i64::from(i) + 1;

        // Simple cell: payload_size || rowid || record_header_size || serial_type || value
        let cell: Vec<u8> = vec![
            4,           // payload size = 4 bytes
            rowid as u8, // rowid (fits in 1-byte varint for small values)
            2,           // record header size = 2
            1,           // serial type 1 = 8-bit signed integer
            val_byte,    // deterministic value from seed
        ];
        let cell_len = cell.len();
        write_pos -= cell_len;
        page[write_pos..write_pos + cell_len].copy_from_slice(&cell);
        cell_offsets.push(write_pos as u16);
    }

    // Page header.
    page[0] = PageType::LeafTable.flag_byte();
    page[1] = 0;
    page[2] = 0;
    page[3] = (cell_count >> 8) as u8;
    page[4] = (cell_count & 0xFF) as u8;
    let co = write_pos as u16;
    page[5] = (co >> 8) as u8;
    page[6] = (co & 0xFF) as u8;
    page[7] = 0;

    // Cell pointer array.
    let ptr_start = header_size as usize;
    for (i, &offset) in cell_offsets.iter().enumerate() {
        let pos = ptr_start + i * 2;
        page[pos] = (offset >> 8) as u8;
        page[pos + 1] = (offset & 0xFF) as u8;
    }

    page
}

// ─── SQL Statement Fixtures ─────────────────────────────────────────────

/// A categorized SQL statement fixture for deterministic testing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqlFixture {
    /// Human-readable identifier.
    pub id: String,
    /// Taxonomy family this fixture targets.
    pub family: Family,
    /// The SQL statements (in execution order).
    pub statements: Vec<String>,
    /// The seed used to select/generate this fixture.
    pub seed: FixtureSeed,
    /// Description of what this fixture tests.
    pub description: String,
    /// Expected behavior tags (e.g., "should-succeed", "should-error").
    pub behavior_tags: Vec<String>,
}

/// Build the canonical SQL statement fixture catalog.
///
/// Covers all 8 taxonomy families with deterministic, minimal test cases.
/// Each fixture is self-contained and can be run independently.
#[must_use]
#[allow(clippy::too_many_lines, clippy::vec_init_then_push)]
pub fn build_sql_fixtures() -> Vec<SqlFixture> {
    let mut fixtures = Vec::new();

    // ── Family::SQL ─────────────────────────────────────────────────

    fixtures.push(SqlFixture {
        id: "sql-create-table-basic".to_owned(),
        family: Family::SQL,
        statements: vec![
            "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);".to_owned(),
        ],
        seed: FixtureSeed::derive_with(FIXTURE_SEED_BASE, domain::SQL, "create_table_basic"),
        description: "Basic CREATE TABLE with typed columns".to_owned(),
        behavior_tags: vec!["should-succeed".to_owned(), "ddl".to_owned()],
    });

    fixtures.push(SqlFixture {
        id: "sql-insert-values".to_owned(),
        family: Family::SQL,
        statements: vec![
            "CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT);".to_owned(),
            "INSERT INTO t1 VALUES (1, 'hello');".to_owned(),
            "INSERT INTO t1 VALUES (2, 'world');".to_owned(),
            "SELECT * FROM t1 ORDER BY id;".to_owned(),
        ],
        seed: FixtureSeed::derive_with(FIXTURE_SEED_BASE, domain::SQL, "insert_values"),
        description: "INSERT with explicit values and SELECT verification".to_owned(),
        behavior_tags: vec!["should-succeed".to_owned(), "dml".to_owned()],
    });

    fixtures.push(SqlFixture {
        id: "sql-update-where".to_owned(),
        family: Family::SQL,
        statements: vec![
            "CREATE TABLE t1 (id INTEGER PRIMARY KEY, val INTEGER);".to_owned(),
            "INSERT INTO t1 VALUES (1, 10);".to_owned(),
            "INSERT INTO t1 VALUES (2, 20);".to_owned(),
            "UPDATE t1 SET val = 99 WHERE id = 1;".to_owned(),
            "SELECT val FROM t1 WHERE id = 1;".to_owned(),
        ],
        seed: FixtureSeed::derive_with(FIXTURE_SEED_BASE, domain::SQL, "update_where"),
        description: "UPDATE with WHERE clause".to_owned(),
        behavior_tags: vec!["should-succeed".to_owned(), "dml".to_owned()],
    });

    fixtures.push(SqlFixture {
        id: "sql-delete-where".to_owned(),
        family: Family::SQL,
        statements: vec![
            "CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT);".to_owned(),
            "INSERT INTO t1 VALUES (1, 'a');".to_owned(),
            "INSERT INTO t1 VALUES (2, 'b');".to_owned(),
            "DELETE FROM t1 WHERE id = 1;".to_owned(),
            "SELECT COUNT(*) FROM t1;".to_owned(),
        ],
        seed: FixtureSeed::derive_with(FIXTURE_SEED_BASE, domain::SQL, "delete_where"),
        description: "DELETE with WHERE clause and count verification".to_owned(),
        behavior_tags: vec!["should-succeed".to_owned(), "dml".to_owned()],
    });

    fixtures.push(SqlFixture {
        id: "sql-compound-union".to_owned(),
        family: Family::SQL,
        statements: vec!["SELECT 1 AS x UNION SELECT 2 UNION SELECT 3 ORDER BY x;".to_owned()],
        seed: FixtureSeed::derive_with(FIXTURE_SEED_BASE, domain::SQL, "compound_union"),
        description: "Compound SELECT with UNION and ORDER BY".to_owned(),
        behavior_tags: vec!["should-succeed".to_owned(), "compound".to_owned()],
    });

    fixtures.push(SqlFixture {
        id: "sql-null-handling".to_owned(),
        family: Family::SQL,
        statements: vec![
            "SELECT NULL IS NULL;".to_owned(),
            "SELECT NULL = NULL;".to_owned(),
            "SELECT COALESCE(NULL, 42);".to_owned(),
            "SELECT IFNULL(NULL, 'fallback');".to_owned(),
        ],
        seed: FixtureSeed::derive_with(FIXTURE_SEED_BASE, domain::SQL, "null_handling"),
        description: "NULL comparison and coalescing semantics".to_owned(),
        behavior_tags: vec!["should-succeed".to_owned(), "null".to_owned()],
    });

    // ── Family::TXN ─────────────────────────────────────────────────

    fixtures.push(SqlFixture {
        id: "txn-autocommit".to_owned(),
        family: Family::TXN,
        statements: vec![
            "CREATE TABLE t1 (id INTEGER PRIMARY KEY);".to_owned(),
            "INSERT INTO t1 VALUES (1);".to_owned(),
            "SELECT * FROM t1;".to_owned(),
        ],
        seed: FixtureSeed::derive_with(FIXTURE_SEED_BASE, domain::SQL, "txn_autocommit"),
        description: "Implicit autocommit behavior".to_owned(),
        behavior_tags: vec!["should-succeed".to_owned(), "autocommit".to_owned()],
    });

    fixtures.push(SqlFixture {
        id: "txn-begin-commit".to_owned(),
        family: Family::TXN,
        statements: vec![
            "CREATE TABLE t1 (id INTEGER PRIMARY KEY);".to_owned(),
            "BEGIN;".to_owned(),
            "INSERT INTO t1 VALUES (1);".to_owned(),
            "INSERT INTO t1 VALUES (2);".to_owned(),
            "COMMIT;".to_owned(),
            "SELECT COUNT(*) FROM t1;".to_owned(),
        ],
        seed: FixtureSeed::derive_with(FIXTURE_SEED_BASE, domain::SQL, "txn_begin_commit"),
        description: "Explicit BEGIN/COMMIT transaction".to_owned(),
        behavior_tags: vec!["should-succeed".to_owned(), "explicit-txn".to_owned()],
    });

    fixtures.push(SqlFixture {
        id: "txn-rollback".to_owned(),
        family: Family::TXN,
        statements: vec![
            "CREATE TABLE t1 (id INTEGER PRIMARY KEY);".to_owned(),
            "INSERT INTO t1 VALUES (1);".to_owned(),
            "BEGIN;".to_owned(),
            "INSERT INTO t1 VALUES (2);".to_owned(),
            "ROLLBACK;".to_owned(),
            "SELECT COUNT(*) FROM t1;".to_owned(),
        ],
        seed: FixtureSeed::derive_with(FIXTURE_SEED_BASE, domain::SQL, "txn_rollback"),
        description: "ROLLBACK discards uncommitted changes".to_owned(),
        behavior_tags: vec!["should-succeed".to_owned(), "rollback".to_owned()],
    });

    fixtures.push(SqlFixture {
        id: "txn-savepoint".to_owned(),
        family: Family::TXN,
        statements: vec![
            "CREATE TABLE t1 (id INTEGER PRIMARY KEY);".to_owned(),
            "BEGIN;".to_owned(),
            "INSERT INTO t1 VALUES (1);".to_owned(),
            "SAVEPOINT sp1;".to_owned(),
            "INSERT INTO t1 VALUES (2);".to_owned(),
            "ROLLBACK TO sp1;".to_owned(),
            "INSERT INTO t1 VALUES (3);".to_owned(),
            "RELEASE sp1;".to_owned(),
            "COMMIT;".to_owned(),
            "SELECT * FROM t1 ORDER BY id;".to_owned(),
        ],
        seed: FixtureSeed::derive_with(FIXTURE_SEED_BASE, domain::SQL, "txn_savepoint"),
        description: "SAVEPOINT with partial ROLLBACK TO and RELEASE".to_owned(),
        behavior_tags: vec!["should-succeed".to_owned(), "savepoint".to_owned()],
    });

    // ── Family::FUN ─────────────────────────────────────────────────

    fixtures.push(SqlFixture {
        id: "fun-scalar-basic".to_owned(),
        family: Family::FUN,
        statements: vec![
            "SELECT abs(-42);".to_owned(),
            "SELECT length('hello');".to_owned(),
            "SELECT typeof(42);".to_owned(),
            "SELECT upper('hello');".to_owned(),
            "SELECT lower('WORLD');".to_owned(),
        ],
        seed: FixtureSeed::derive_with(FIXTURE_SEED_BASE, domain::SQL, "fun_scalar_basic"),
        description: "Basic scalar functions: abs, length, typeof, upper, lower".to_owned(),
        behavior_tags: vec!["should-succeed".to_owned(), "scalar".to_owned()],
    });

    fixtures.push(SqlFixture {
        id: "fun-aggregate-basic".to_owned(),
        family: Family::FUN,
        statements: vec![
            "CREATE TABLE t1 (val INTEGER);".to_owned(),
            "INSERT INTO t1 VALUES (10);".to_owned(),
            "INSERT INTO t1 VALUES (20);".to_owned(),
            "INSERT INTO t1 VALUES (30);".to_owned(),
            "SELECT COUNT(*), SUM(val), AVG(val), MIN(val), MAX(val) FROM t1;".to_owned(),
        ],
        seed: FixtureSeed::derive_with(FIXTURE_SEED_BASE, domain::SQL, "fun_aggregate_basic"),
        description: "Basic aggregate functions: COUNT, SUM, AVG, MIN, MAX".to_owned(),
        behavior_tags: vec!["should-succeed".to_owned(), "aggregate".to_owned()],
    });

    fixtures.push(SqlFixture {
        id: "fun-string-ops".to_owned(),
        family: Family::FUN,
        statements: vec![
            "SELECT substr('hello world', 7, 5);".to_owned(),
            "SELECT replace('hello', 'l', 'r');".to_owned(),
            "SELECT trim('  hello  ');".to_owned(),
            "SELECT hex(X'CAFE');".to_owned(),
            "SELECT 'hello' || ' ' || 'world';".to_owned(),
        ],
        seed: FixtureSeed::derive_with(FIXTURE_SEED_BASE, domain::SQL, "fun_string_ops"),
        description: "String functions: substr, replace, trim, hex, concat".to_owned(),
        behavior_tags: vec!["should-succeed".to_owned(), "string".to_owned()],
    });

    // ── Family::VDB ─────────────────────────────────────────────────

    fixtures.push(SqlFixture {
        id: "vdb-explain-select".to_owned(),
        family: Family::VDB,
        statements: vec![
            "CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT);".to_owned(),
            "EXPLAIN SELECT val FROM t1 WHERE id = 1;".to_owned(),
        ],
        seed: FixtureSeed::derive_with(FIXTURE_SEED_BASE, domain::SQL, "vdb_explain_select"),
        description: "EXPLAIN output for simple SELECT with WHERE".to_owned(),
        behavior_tags: vec!["should-succeed".to_owned(), "explain".to_owned()],
    });

    fixtures.push(SqlFixture {
        id: "vdb-explain-qp".to_owned(),
        family: Family::VDB,
        statements: vec![
            "CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT);".to_owned(),
            "EXPLAIN QUERY PLAN SELECT val FROM t1 WHERE id > 5;".to_owned(),
        ],
        seed: FixtureSeed::derive_with(FIXTURE_SEED_BASE, domain::SQL, "vdb_explain_qp"),
        description: "EXPLAIN QUERY PLAN output for range scan".to_owned(),
        behavior_tags: vec!["should-succeed".to_owned(), "explain-qp".to_owned()],
    });

    // ── Family::PLN ─────────────────────────────────────────────────

    fixtures.push(SqlFixture {
        id: "pln-join-two-tables".to_owned(),
        family: Family::PLN,
        statements: vec![
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);".to_owned(),
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount REAL);"
                .to_owned(),
            "INSERT INTO users VALUES (1, 'Alice');".to_owned(),
            "INSERT INTO orders VALUES (1, 1, 9.99);".to_owned(),
            "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id;".to_owned(),
        ],
        seed: FixtureSeed::derive_with(FIXTURE_SEED_BASE, domain::SQL, "pln_join_two"),
        description: "Two-table JOIN with equality predicate".to_owned(),
        behavior_tags: vec!["should-succeed".to_owned(), "join".to_owned()],
    });

    fixtures.push(SqlFixture {
        id: "pln-subquery-in".to_owned(),
        family: Family::PLN,
        statements: vec![
            "CREATE TABLE t1 (id INTEGER PRIMARY KEY);".to_owned(),
            "INSERT INTO t1 VALUES (1);".to_owned(),
            "INSERT INTO t1 VALUES (2);".to_owned(),
            "INSERT INTO t1 VALUES (3);".to_owned(),
            "SELECT * FROM t1 WHERE id IN (SELECT id FROM t1 WHERE id > 1);".to_owned(),
        ],
        seed: FixtureSeed::derive_with(FIXTURE_SEED_BASE, domain::SQL, "pln_subquery_in"),
        description: "Subquery with IN clause".to_owned(),
        behavior_tags: vec!["should-succeed".to_owned(), "subquery".to_owned()],
    });

    fixtures.push(SqlFixture {
        id: "pln-cte-recursive".to_owned(),
        family: Family::PLN,
        statements: vec![
            "WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x < 5) SELECT x FROM cnt;".to_owned(),
        ],
        seed: FixtureSeed::derive_with(FIXTURE_SEED_BASE, domain::SQL, "pln_cte_recursive"),
        description: "Recursive CTE generating sequence 1..5".to_owned(),
        behavior_tags: vec!["should-succeed".to_owned(), "cte".to_owned()],
    });

    // ── Family::PGM ─────────────────────────────────────────────────

    fixtures.push(SqlFixture {
        id: "pgm-pragma-table-info".to_owned(),
        family: Family::PGM,
        statements: vec![
            "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER DEFAULT 0);"
                .to_owned(),
            "PRAGMA table_info(t1);".to_owned(),
        ],
        seed: FixtureSeed::derive_with(FIXTURE_SEED_BASE, domain::SQL, "pgm_table_info"),
        description: "PRAGMA table_info introspection".to_owned(),
        behavior_tags: vec!["should-succeed".to_owned(), "pragma".to_owned()],
    });

    fixtures.push(SqlFixture {
        id: "pgm-pragma-integrity-check".to_owned(),
        family: Family::PGM,
        statements: vec![
            "CREATE TABLE t1 (id INTEGER PRIMARY KEY);".to_owned(),
            "INSERT INTO t1 VALUES (1);".to_owned(),
            "PRAGMA integrity_check;".to_owned(),
        ],
        seed: FixtureSeed::derive_with(FIXTURE_SEED_BASE, domain::SQL, "pgm_integrity_check"),
        description: "PRAGMA integrity_check on simple database".to_owned(),
        behavior_tags: vec!["should-succeed".to_owned(), "pragma".to_owned()],
    });

    // ── Family::EXT ─────────────────────────────────────────────────

    fixtures.push(SqlFixture {
        id: "ext-json-extract".to_owned(),
        family: Family::EXT,
        statements: vec![
            r#"SELECT json_extract('{"a":1,"b":"two"}', '$.a');"#.to_owned(),
            r#"SELECT json_extract('{"a":1,"b":"two"}', '$.b');"#.to_owned(),
            r#"SELECT json_type('{"a":1}', '$.a');"#.to_owned(),
        ],
        seed: FixtureSeed::derive_with(FIXTURE_SEED_BASE, domain::SQL, "ext_json_extract"),
        description: "JSON1 extension: json_extract and json_type".to_owned(),
        behavior_tags: vec!["should-succeed".to_owned(), "json".to_owned()],
    });

    fixtures.push(SqlFixture {
        id: "ext-generate-series".to_owned(),
        family: Family::EXT,
        statements: vec![
            "SELECT value FROM generate_series(1, 5);".to_owned(),
            "SELECT value FROM generate_series(0, 10, 3);".to_owned(),
        ],
        seed: FixtureSeed::derive_with(FIXTURE_SEED_BASE, domain::SQL, "ext_generate_series"),
        description: "generate_series virtual table function".to_owned(),
        behavior_tags: vec!["should-succeed".to_owned(), "vtab".to_owned()],
    });

    // ── Family::CLI ─────────────────────────────────────────────────

    fixtures.push(SqlFixture {
        id: "cli-dot-schema".to_owned(),
        family: Family::CLI,
        statements: vec![
            "CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT);".to_owned(),
            ".schema".to_owned(),
        ],
        seed: FixtureSeed::derive_with(FIXTURE_SEED_BASE, domain::SQL, "cli_dot_schema"),
        description: "CLI .schema dot-command".to_owned(),
        behavior_tags: vec!["should-succeed".to_owned(), "dot-command".to_owned()],
    });

    fixtures.push(SqlFixture {
        id: "cli-dot-tables".to_owned(),
        family: Family::CLI,
        statements: vec![
            "CREATE TABLE t1 (id INTEGER PRIMARY KEY);".to_owned(),
            "CREATE TABLE t2 (id INTEGER PRIMARY KEY);".to_owned(),
            ".tables".to_owned(),
        ],
        seed: FixtureSeed::derive_with(FIXTURE_SEED_BASE, domain::SQL, "cli_dot_tables"),
        description: "CLI .tables dot-command".to_owned(),
        behavior_tags: vec!["should-succeed".to_owned(), "dot-command".to_owned()],
    });

    fixtures
}

// ─── Value Fixtures ─────────────────────────────────────────────────────

/// A fixture providing deterministic `SqliteValue` instances.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValueFixture {
    /// Identifier for this fixture.
    pub id: String,
    /// The storage class being tested.
    pub storage_class: String,
    /// JSON representation of the value (for serialization roundtrip testing).
    pub json_repr: String,
    /// Description.
    pub description: String,
    /// The seed used to generate this fixture.
    pub seed: FixtureSeed,
}

/// Build the canonical value fixture catalog covering all five storage classes.
#[must_use]
pub fn build_value_fixtures() -> Vec<ValueFixture> {
    let mut fixtures = Vec::new();

    let values: Vec<(&str, &str, &str, &str)> = vec![
        ("val-null", "NULL", "null", "SQL NULL value"),
        ("val-int-zero", "INTEGER", "0", "Integer zero"),
        ("val-int-positive", "INTEGER", "42", "Positive integer"),
        ("val-int-negative", "INTEGER", "-17", "Negative integer"),
        ("val-int-max", "INTEGER", "9223372036854775807", "i64::MAX"),
        ("val-int-min", "INTEGER", "-9223372036854775808", "i64::MIN"),
        ("val-real-zero", "REAL", "0.0", "Float zero"),
        ("val-real-pi", "REAL", "3.141592653589793", "Float pi"),
        (
            "val-real-negative",
            "REAL",
            "-2.718281828",
            "Negative float",
        ),
        ("val-text-empty", "TEXT", "\"\"", "Empty string"),
        ("val-text-hello", "TEXT", "\"hello\"", "Simple ASCII string"),
        (
            "val-text-unicode",
            "TEXT",
            "\"caf\\u00e9\"",
            "Unicode string with accented character",
        ),
        ("val-blob-empty", "BLOB", "[]", "Empty blob"),
        (
            "val-blob-bytes",
            "BLOB",
            "[202,254,186,190]",
            "4-byte blob (0xCAFEBABE)",
        ),
    ];

    for (i, (id, class, json, desc)) in values.into_iter().enumerate() {
        let seed =
            FixtureSeed::derive_with(FIXTURE_SEED_BASE, domain::VALUE, &format!("value_{i}"));
        fixtures.push(ValueFixture {
            id: id.to_owned(),
            storage_class: class.to_owned(),
            json_repr: json.to_owned(),
            description: desc.to_owned(),
            seed,
        });
    }

    fixtures
}

// ─── Trace Fixtures ─────────────────────────────────────────────────────

/// A VDBE execution trace fixture — a sequence of opcodes that should be
/// produced for a given SQL input.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceFixture {
    /// Identifier for this fixture.
    pub id: String,
    /// The input SQL that produces this trace.
    pub input_sql: String,
    /// Expected opcode sequence (opcode names in execution order).
    /// These are the key opcodes — the trace may contain additional ops.
    pub expected_key_opcodes: Vec<String>,
    /// Seed for this fixture.
    pub seed: FixtureSeed,
    /// Description.
    pub description: String,
}

/// Build the canonical VDBE trace fixture catalog.
///
/// Each fixture specifies a SQL statement and the key opcodes expected in
/// the compiled bytecode. This tests the codegen pipeline's correctness.
#[must_use]
pub fn build_trace_fixtures() -> Vec<TraceFixture> {
    vec![
        TraceFixture {
            id: "trace-simple-select-const".to_owned(),
            input_sql: "SELECT 42;".to_owned(),
            expected_key_opcodes: vec![
                "Init".to_owned(),
                "Integer".to_owned(),
                "ResultRow".to_owned(),
                "Halt".to_owned(),
            ],
            seed: FixtureSeed::derive_with(FIXTURE_SEED_BASE, domain::TRACE, "simple_select_const"),
            description: "Constant SELECT should compile to Init → Integer → ResultRow → Halt"
                .to_owned(),
        },
        TraceFixture {
            id: "trace-select-from-table".to_owned(),
            input_sql: "SELECT id FROM t1;".to_owned(),
            expected_key_opcodes: vec![
                "Init".to_owned(),
                "OpenRead".to_owned(),
                "Rewind".to_owned(),
                "Column".to_owned(),
                "ResultRow".to_owned(),
                "Next".to_owned(),
                "Halt".to_owned(),
            ],
            seed: FixtureSeed::derive_with(FIXTURE_SEED_BASE, domain::TRACE, "select_from_table"),
            description: "Table scan should use OpenRead → Rewind → Column → ResultRow → Next loop"
                .to_owned(),
        },
        TraceFixture {
            id: "trace-insert-single".to_owned(),
            input_sql: "INSERT INTO t1 VALUES (1, 'hello');".to_owned(),
            expected_key_opcodes: vec![
                "Init".to_owned(),
                "OpenWrite".to_owned(),
                "NewRowid".to_owned(),
                "String8".to_owned(),
                "MakeRecord".to_owned(),
                "Insert".to_owned(),
                "Halt".to_owned(),
            ],
            seed: FixtureSeed::derive_with(FIXTURE_SEED_BASE, domain::TRACE, "insert_single"),
            description: "Single INSERT should use OpenWrite → NewRowid → MakeRecord → Insert"
                .to_owned(),
        },
        TraceFixture {
            id: "trace-delete-where".to_owned(),
            input_sql: "DELETE FROM t1 WHERE id = 1;".to_owned(),
            expected_key_opcodes: vec![
                "Init".to_owned(),
                "OpenWrite".to_owned(),
                "SeekRowid".to_owned(),
                "Delete".to_owned(),
                "Halt".to_owned(),
            ],
            seed: FixtureSeed::derive_with(FIXTURE_SEED_BASE, domain::TRACE, "delete_where"),
            description: "DELETE by rowid should use SeekRowid → Delete".to_owned(),
        },
        TraceFixture {
            id: "trace-aggregate-count".to_owned(),
            input_sql: "SELECT COUNT(*) FROM t1;".to_owned(),
            expected_key_opcodes: vec![
                "Init".to_owned(),
                "OpenRead".to_owned(),
                "Rewind".to_owned(),
                "AggStep".to_owned(),
                "AggFinal".to_owned(),
                "ResultRow".to_owned(),
                "Halt".to_owned(),
            ],
            seed: FixtureSeed::derive_with(FIXTURE_SEED_BASE, domain::TRACE, "aggregate_count"),
            description: "COUNT(*) should use AggStep → AggFinal".to_owned(),
        },
    ]
}

// ─── Fixture Catalog ────────────────────────────────────────────────────

/// The complete fixture catalog — a single entry point for all fixture types.
///
/// Build once, use everywhere. The catalog is deterministic: calling
/// [`FixtureCatalog::build`] always produces the same output.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FixtureCatalog {
    /// Bead ID for traceability.
    pub bead_id: String,
    /// Root seed used to derive all fixtures.
    pub root_seed: u64,
    /// Storage page fixtures.
    pub page_fixtures: Vec<PageFixture>,
    /// SQL statement fixtures.
    pub sql_fixtures: Vec<SqlFixture>,
    /// Value fixtures covering all storage classes.
    pub value_fixtures: Vec<ValueFixture>,
    /// VDBE trace fixtures.
    pub trace_fixtures: Vec<TraceFixture>,
}

impl FixtureCatalog {
    /// Build the complete fixture catalog.
    ///
    /// This is deterministic — every call produces identical output.
    #[must_use]
    pub fn build() -> Self {
        Self {
            bead_id: BEAD_ID.to_owned(),
            root_seed: FIXTURE_SEED_BASE,
            page_fixtures: build_page_fixtures(),
            sql_fixtures: build_sql_fixtures(),
            value_fixtures: build_value_fixtures(),
            trace_fixtures: build_trace_fixtures(),
        }
    }

    /// Total number of fixtures across all categories.
    #[must_use]
    pub fn total_count(&self) -> usize {
        self.page_fixtures.len()
            + self.sql_fixtures.len()
            + self.value_fixtures.len()
            + self.trace_fixtures.len()
    }

    /// Summary statistics as a map of category → count.
    #[must_use]
    pub fn summary(&self) -> BTreeMap<String, usize> {
        let mut m = BTreeMap::new();
        m.insert("page".to_owned(), self.page_fixtures.len());
        m.insert("sql".to_owned(), self.sql_fixtures.len());
        m.insert("value".to_owned(), self.value_fixtures.len());
        m.insert("trace".to_owned(), self.trace_fixtures.len());
        m.insert("total".to_owned(), self.total_count());
        m
    }

    /// Get SQL fixtures filtered by taxonomy family.
    #[must_use]
    pub fn sql_by_family(&self, family: Family) -> Vec<&SqlFixture> {
        self.sql_fixtures
            .iter()
            .filter(|f| f.family == family)
            .collect()
    }

    /// Get page fixtures filtered by page type.
    #[must_use]
    pub fn pages_by_type(&self, page_type: PageType) -> Vec<&PageFixture> {
        self.page_fixtures
            .iter()
            .filter(|f| f.page_type == page_type)
            .collect()
    }

    /// Verify all fixture IDs are unique.
    #[must_use]
    pub fn validate_unique_ids(&self) -> Vec<String> {
        let mut seen = std::collections::HashSet::new();
        let mut dupes = Vec::new();

        for f in &self.page_fixtures {
            if !seen.insert(&f.id) {
                dupes.push(f.id.clone());
            }
        }
        for f in &self.sql_fixtures {
            if !seen.insert(&f.id) {
                dupes.push(f.id.clone());
            }
        }
        for f in &self.value_fixtures {
            if !seen.insert(&f.id) {
                dupes.push(f.id.clone());
            }
        }
        for f in &self.trace_fixtures {
            if !seen.insert(&f.id) {
                dupes.push(f.id.clone());
            }
        }

        dupes
    }
}

// ─── Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── Seed contract tests ─────────────────────────────────────────

    #[test]
    fn seed_derivation_is_deterministic() {
        let a = FixtureSeed::derive("test_scope_alpha");
        let b = FixtureSeed::derive("test_scope_alpha");
        assert_eq!(
            a, b,
            "bead_id={BEAD_ID} case=seed_determinism: same inputs must produce same seed"
        );
    }

    #[test]
    fn seed_derivation_varies_by_scope() {
        let a = FixtureSeed::derive("scope_a");
        let b = FixtureSeed::derive("scope_b");
        assert_ne!(
            a, b,
            "bead_id={BEAD_ID} case=seed_scope_isolation: different scopes must differ"
        );
    }

    #[test]
    fn seed_derivation_varies_by_domain() {
        let a = FixtureSeed::derive_with(FIXTURE_SEED_BASE, domain::PAGE, "same");
        let b = FixtureSeed::derive_with(FIXTURE_SEED_BASE, domain::SQL, "same");
        assert_ne!(
            a, b,
            "bead_id={BEAD_ID} case=seed_domain_isolation: different domains must differ"
        );
    }

    #[test]
    fn seed_child_derivation_is_deterministic() {
        let parent = FixtureSeed::derive("parent");
        let c1 = parent.child(0);
        let c2 = parent.child(0);
        assert_eq!(c1, c2, "bead_id={BEAD_ID} case=seed_child_determinism");
    }

    #[test]
    fn seed_children_are_distinct() {
        let parent = FixtureSeed::derive("parent");
        let c0 = parent.child(0);
        let c1 = parent.child(1);
        let c2 = parent.child(2);
        assert_ne!(c0, c1, "bead_id={BEAD_ID} case=seed_child_distinct_0_1");
        assert_ne!(c1, c2, "bead_id={BEAD_ID} case=seed_child_distinct_1_2");
        assert_ne!(c0, c2, "bead_id={BEAD_ID} case=seed_child_distinct_0_2");
    }

    #[test]
    fn seed_display_is_hex() {
        let seed = FixtureSeed(0xDEAD_BEEF_CAFE_BABE);
        let s = format!("{seed}");
        assert!(
            s.starts_with("seed:0x"),
            "bead_id={BEAD_ID} case=seed_display: got {s}"
        );
    }

    // ── Page fixture tests ──────────────────────────────────────────

    #[test]
    fn page_fixtures_are_deterministic() {
        let a = build_page_fixtures();
        let b = build_page_fixtures();
        assert_eq!(
            a.len(),
            b.len(),
            "bead_id={BEAD_ID} case=page_determinism_count"
        );
        for (fa, fb) in a.iter().zip(b.iter()) {
            assert_eq!(
                fa.data, fb.data,
                "bead_id={BEAD_ID} case=page_determinism_data fixture={}",
                fa.id
            );
        }
    }

    #[test]
    fn page_fixtures_have_valid_headers() {
        for fixture in &build_page_fixtures() {
            assert!(
                !fixture.data.is_empty(),
                "bead_id={BEAD_ID} case=page_nonempty fixture={}",
                fixture.id
            );
            assert_eq!(
                fixture.data.len(),
                fixture.page_size as usize,
                "bead_id={BEAD_ID} case=page_size_match fixture={}",
                fixture.id
            );
            assert_eq!(
                fixture.data[0],
                fixture.page_type.flag_byte(),
                "bead_id={BEAD_ID} case=page_flag_byte fixture={}",
                fixture.id
            );
        }
    }

    #[test]
    fn page_fixture_cell_count_matches_header() {
        for fixture in &build_page_fixtures() {
            let header_count = u16::from(fixture.data[3]) << 8 | u16::from(fixture.data[4]);
            assert_eq!(
                header_count, fixture.cell_count,
                "bead_id={BEAD_ID} case=page_cell_count fixture={}",
                fixture.id
            );
        }
    }

    #[test]
    fn page_fixture_covers_all_types() {
        let fixtures = build_page_fixtures();
        for pt in PageType::ALL {
            let count = fixtures.iter().filter(|f| f.page_type == pt).count();
            assert!(
                count > 0,
                "bead_id={BEAD_ID} case=page_type_coverage type={pt}: no fixtures found"
            );
        }
    }

    #[test]
    fn page_fixture_boundary_sizes() {
        let fixtures = build_page_fixtures();
        let sizes: Vec<u32> = fixtures.iter().map(|f| f.page_size).collect();
        assert!(
            sizes.contains(&512),
            "bead_id={BEAD_ID} case=page_min_size: missing 512-byte page"
        );
        assert!(
            sizes.contains(&4096),
            "bead_id={BEAD_ID} case=page_default_size: missing 4096-byte page"
        );
        assert!(
            sizes.contains(&65_536),
            "bead_id={BEAD_ID} case=page_max_size: missing 65536-byte page"
        );
    }

    // ── SQL fixture tests ───────────────────────────────────────────

    #[test]
    fn sql_fixtures_are_deterministic() {
        let a = build_sql_fixtures();
        let b = build_sql_fixtures();
        assert_eq!(
            a.len(),
            b.len(),
            "bead_id={BEAD_ID} case=sql_determinism_count"
        );
        for (fa, fb) in a.iter().zip(b.iter()) {
            assert_eq!(
                fa.statements, fb.statements,
                "bead_id={BEAD_ID} case=sql_determinism_stmts fixture={}",
                fa.id
            );
            assert_eq!(
                fa.seed, fb.seed,
                "bead_id={BEAD_ID} case=sql_determinism_seed fixture={}",
                fa.id
            );
        }
    }

    #[test]
    fn sql_fixtures_cover_all_families() {
        let fixtures = build_sql_fixtures();
        for family in Family::ALL {
            let count = fixtures.iter().filter(|f| f.family == family).count();
            assert!(
                count > 0,
                "bead_id={BEAD_ID} case=sql_family_coverage family={family:?}: no fixtures"
            );
        }
    }

    #[test]
    fn sql_fixtures_have_nonempty_statements() {
        for fixture in &build_sql_fixtures() {
            assert!(
                !fixture.statements.is_empty(),
                "bead_id={BEAD_ID} case=sql_nonempty_stmts fixture={}",
                fixture.id
            );
            for (i, stmt) in fixture.statements.iter().enumerate() {
                assert!(
                    !stmt.is_empty(),
                    "bead_id={BEAD_ID} case=sql_nonempty_stmt fixture={} index={i}",
                    fixture.id
                );
            }
        }
    }

    #[test]
    fn sql_fixtures_have_behavior_tags() {
        for fixture in &build_sql_fixtures() {
            assert!(
                !fixture.behavior_tags.is_empty(),
                "bead_id={BEAD_ID} case=sql_has_tags fixture={}",
                fixture.id
            );
        }
    }

    // ── Value fixture tests ─────────────────────────────────────────

    #[test]
    fn value_fixtures_cover_all_classes() {
        let fixtures = build_value_fixtures();
        let classes: Vec<&str> = fixtures.iter().map(|f| f.storage_class.as_str()).collect();
        for expected in ["NULL", "INTEGER", "REAL", "TEXT", "BLOB"] {
            assert!(
                classes.contains(&expected),
                "bead_id={BEAD_ID} case=value_class_coverage class={expected}: not found"
            );
        }
    }

    #[test]
    fn value_fixtures_are_deterministic() {
        let a = build_value_fixtures();
        let b = build_value_fixtures();
        for (fa, fb) in a.iter().zip(b.iter()) {
            assert_eq!(fa.id, fb.id, "bead_id={BEAD_ID} case=value_determinism_id");
            assert_eq!(
                fa.json_repr, fb.json_repr,
                "bead_id={BEAD_ID} case=value_determinism_json fixture={}",
                fa.id
            );
            assert_eq!(
                fa.seed, fb.seed,
                "bead_id={BEAD_ID} case=value_determinism_seed fixture={}",
                fa.id
            );
        }
    }

    // ── Trace fixture tests ─────────────────────────────────────────

    #[test]
    fn trace_fixtures_are_deterministic() {
        let a = build_trace_fixtures();
        let b = build_trace_fixtures();
        assert_eq!(
            a.len(),
            b.len(),
            "bead_id={BEAD_ID} case=trace_determinism_count"
        );
        for (fa, fb) in a.iter().zip(b.iter()) {
            assert_eq!(
                fa.expected_key_opcodes, fb.expected_key_opcodes,
                "bead_id={BEAD_ID} case=trace_determinism_opcodes fixture={}",
                fa.id
            );
        }
    }

    #[test]
    fn trace_fixtures_have_valid_opcodes() {
        let valid_opcodes = [
            "Init",
            "Goto",
            "Halt",
            "Integer",
            "Int64",
            "Real",
            "String8",
            "Null",
            "ResultRow",
            "OpenRead",
            "OpenWrite",
            "Rewind",
            "Next",
            "Column",
            "MakeRecord",
            "Insert",
            "Delete",
            "SeekRowid",
            "SeekGE",
            "SeekGT",
            "SeekLE",
            "SeekLT",
            "NewRowid",
            "AggStep",
            "AggFinal",
            "Copy",
            "SCopy",
            "Move",
            "Function",
            "Compare",
            "Jump",
        ];
        for fixture in &build_trace_fixtures() {
            for op in &fixture.expected_key_opcodes {
                assert!(
                    valid_opcodes.contains(&op.as_str()),
                    "bead_id={BEAD_ID} case=trace_valid_opcode fixture={} opcode={op}",
                    fixture.id
                );
            }
        }
    }

    // ── Catalog tests ───────────────────────────────────────────────

    #[test]
    fn catalog_builds_deterministically() {
        let a = FixtureCatalog::build();
        let b = FixtureCatalog::build();
        assert_eq!(
            a.total_count(),
            b.total_count(),
            "bead_id={BEAD_ID} case=catalog_determinism_total"
        );
        assert_eq!(
            a.summary(),
            b.summary(),
            "bead_id={BEAD_ID} case=catalog_determinism_summary"
        );
    }

    #[test]
    fn catalog_has_unique_ids() {
        let catalog = FixtureCatalog::build();
        let dupes = catalog.validate_unique_ids();
        assert!(
            dupes.is_empty(),
            "bead_id={BEAD_ID} case=catalog_unique_ids: duplicate IDs found: {dupes:?}"
        );
    }

    #[test]
    fn catalog_total_count_matches_sum() {
        let catalog = FixtureCatalog::build();
        let expected = catalog.page_fixtures.len()
            + catalog.sql_fixtures.len()
            + catalog.value_fixtures.len()
            + catalog.trace_fixtures.len();
        assert_eq!(
            catalog.total_count(),
            expected,
            "bead_id={BEAD_ID} case=catalog_total_count"
        );
    }

    #[test]
    fn catalog_summary_keys() {
        let catalog = FixtureCatalog::build();
        let summary = catalog.summary();
        for key in ["page", "sql", "value", "trace", "total"] {
            assert!(
                summary.contains_key(key),
                "bead_id={BEAD_ID} case=catalog_summary_keys: missing key '{key}'"
            );
        }
    }

    #[test]
    fn catalog_filter_sql_by_family() {
        let catalog = FixtureCatalog::build();
        let sql_family = catalog.sql_by_family(Family::SQL);
        assert!(
            !sql_family.is_empty(),
            "bead_id={BEAD_ID} case=catalog_filter_sql_family"
        );
        for f in &sql_family {
            assert_eq!(
                f.family,
                Family::SQL,
                "bead_id={BEAD_ID} case=catalog_filter_family_match"
            );
        }
    }

    #[test]
    fn catalog_filter_pages_by_type() {
        let catalog = FixtureCatalog::build();
        let leaf_tables = catalog.pages_by_type(PageType::LeafTable);
        assert!(
            !leaf_tables.is_empty(),
            "bead_id={BEAD_ID} case=catalog_filter_page_type"
        );
        for f in &leaf_tables {
            assert_eq!(
                f.page_type,
                PageType::LeafTable,
                "bead_id={BEAD_ID} case=catalog_filter_page_type_match"
            );
        }
    }

    #[test]
    fn catalog_json_roundtrip() {
        let catalog = FixtureCatalog::build();
        let json = serde_json::to_string(&catalog).expect("serialize catalog");
        let deserialized: FixtureCatalog =
            serde_json::from_str(&json).expect("deserialize catalog");
        assert_eq!(
            catalog.total_count(),
            deserialized.total_count(),
            "bead_id={BEAD_ID} case=catalog_json_roundtrip"
        );
        assert_eq!(
            catalog.bead_id, deserialized.bead_id,
            "bead_id={BEAD_ID} case=catalog_json_roundtrip_bead_id"
        );
    }

    #[test]
    fn catalog_has_nontrivial_fixture_counts() {
        let catalog = FixtureCatalog::build();
        let summary = catalog.summary();
        assert!(
            summary["page"] >= 7,
            "bead_id={BEAD_ID} case=catalog_page_count: got {}",
            summary["page"]
        );
        assert!(
            summary["sql"] >= 20,
            "bead_id={BEAD_ID} case=catalog_sql_count: got {}",
            summary["sql"]
        );
        assert!(
            summary["value"] >= 14,
            "bead_id={BEAD_ID} case=catalog_value_count: got {}",
            summary["value"]
        );
        assert!(
            summary["trace"] >= 5,
            "bead_id={BEAD_ID} case=catalog_trace_count: got {}",
            summary["trace"]
        );
    }
}
