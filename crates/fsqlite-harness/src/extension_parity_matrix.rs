//! Extension parity contract matrix and feature-flag truth table (bd-1dp9.5.1).
//!
//! Defines the precise extension-surface contract for FrankenSQLite against
//! SQLite 3.52.0.  Every extension module (JSON1, FTS3/4/5, R-tree, Session,
//! ICU, misc) is decomposed into individual surface points (functions, virtual
//! tables, operators) with:
//!
//! - compile-time feature flags that gate each surface,
//! - current implementation status,
//! - intentional omission rationale where applicable,
//! - acceptance test references for CI consumption.
//!
//! # Design
//!
//! The [`ExtensionParityMatrix`] is the top-level structure consumed by CI
//! gates and the parity score engine (bd-1dp9.1.3).  It links back to the
//! canonical feature universe via [`FeatureId`] and provides deterministic
//! JSON serialization for machine-readable reporting.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::parity_taxonomy::{FeatureId, ParityStatus};

/// Bead identifier for log correlation.
#[allow(dead_code)]
const BEAD_ID: &str = "bd-1dp9.5.1";

/// Schema version for forward-compatible migrations.
pub const MATRIX_SCHEMA_VERSION: u32 = 1;

// ---------------------------------------------------------------------------
// Extension module identification
// ---------------------------------------------------------------------------

/// An extension module (crate) in the workspace.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ExtensionModule {
    /// Full-text search v3/v4 (`fsqlite-ext-fts3`).
    Fts3,
    /// Full-text search v5 (`fsqlite-ext-fts5`).
    Fts5,
    /// JSON functions and operators (`fsqlite-ext-json`).
    Json,
    /// R-tree spatial indexing (`fsqlite-ext-rtree`).
    Rtree,
    /// Session changesets and patchsets (`fsqlite-ext-session`).
    Session,
    /// ICU collation and pattern matching (`fsqlite-ext-icu`).
    Icu,
    /// Miscellaneous extensions (`fsqlite-ext-misc`).
    Misc,
}

impl ExtensionModule {
    /// All modules in canonical order.
    pub const ALL: [Self; 7] = [
        Self::Fts3,
        Self::Fts5,
        Self::Json,
        Self::Rtree,
        Self::Session,
        Self::Icu,
        Self::Misc,
    ];

    /// Crate name in the workspace.
    #[must_use]
    pub const fn crate_name(self) -> &'static str {
        match self {
            Self::Fts3 => "fsqlite-ext-fts3",
            Self::Fts5 => "fsqlite-ext-fts5",
            Self::Json => "fsqlite-ext-json",
            Self::Rtree => "fsqlite-ext-rtree",
            Self::Session => "fsqlite-ext-session",
            Self::Icu => "fsqlite-ext-icu",
            Self::Misc => "fsqlite-ext-misc",
        }
    }

    /// Human-readable display name.
    #[must_use]
    pub const fn display_name(self) -> &'static str {
        match self {
            Self::Fts3 => "FTS3/FTS4",
            Self::Fts5 => "FTS5",
            Self::Json => "JSON1",
            Self::Rtree => "R-tree / Geopoly",
            Self::Session => "Session",
            Self::Icu => "ICU",
            Self::Misc => "Miscellaneous",
        }
    }

    /// SQLite compile-time flag that enables this module.
    #[must_use]
    pub const fn sqlite_enable_flag(self) -> &'static str {
        match self {
            Self::Fts3 => "SQLITE_ENABLE_FTS3",
            Self::Fts5 => "SQLITE_ENABLE_FTS5",
            Self::Json => "SQLITE_ENABLE_JSON1",
            Self::Rtree => "SQLITE_ENABLE_RTREE",
            Self::Session => "SQLITE_ENABLE_SESSION",
            Self::Icu => "SQLITE_ENABLE_ICU",
            Self::Misc => "SQLITE_ENABLE_DBSTAT_VTAB",
        }
    }
}

impl fmt::Display for ExtensionModule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.display_name())
    }
}

// ---------------------------------------------------------------------------
// Feature flag truth table
// ---------------------------------------------------------------------------

/// Compile-time feature flag controlling an extension surface.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct FeatureFlag {
    /// Flag name (e.g., `"ext-json"`).
    pub name: String,
    /// Extension module this flag gates.
    pub module: ExtensionModule,
    /// Whether this flag is enabled by default in FrankenSQLite.
    pub enabled_by_default: bool,
    /// Corresponding SQLite compile-time define.
    pub sqlite_define: String,
    /// Description of what this flag controls.
    pub description: String,
}

/// The complete feature-flag truth table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureFlagTable {
    /// Schema version.
    pub schema_version: u32,
    /// Flags keyed by name for deterministic iteration.
    pub flags: BTreeMap<String, FeatureFlag>,
}

impl FeatureFlagTable {
    /// Build the canonical truth table.
    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn canonical() -> Self {
        let mut flags = BTreeMap::new();

        let entries = [
            (
                "ext-fts3",
                ExtensionModule::Fts3,
                true,
                "SQLITE_ENABLE_FTS3",
                "Enable FTS3 full-text search virtual table",
            ),
            (
                "ext-fts3-parenthesis",
                ExtensionModule::Fts3,
                true,
                "SQLITE_ENABLE_FTS3_PARENTHESIS",
                "Enable enhanced query syntax with AND/OR/NOT/parentheses",
            ),
            (
                "ext-fts3-tokenizer",
                ExtensionModule::Fts3,
                true,
                "SQLITE_ENABLE_FTS3_TOKENIZER",
                "Enable fts3_tokenizer() interface for custom tokenizers",
            ),
            (
                "ext-fts4",
                ExtensionModule::Fts3,
                true,
                "SQLITE_ENABLE_FTS4",
                "Enable FTS4 enhancements (content tables, languageid)",
            ),
            (
                "ext-fts5",
                ExtensionModule::Fts5,
                true,
                "SQLITE_ENABLE_FTS5",
                "Enable FTS5 full-text search with BM25 ranking",
            ),
            (
                "ext-json1",
                ExtensionModule::Json,
                true,
                "SQLITE_ENABLE_JSON1",
                "Enable JSON1 functions and table-valued decomposition",
            ),
            (
                "ext-rtree",
                ExtensionModule::Rtree,
                true,
                "SQLITE_ENABLE_RTREE",
                "Enable R-tree spatial index virtual table",
            ),
            (
                "ext-geopoly",
                ExtensionModule::Rtree,
                false,
                "SQLITE_ENABLE_GEOPOLY",
                "Enable Geopoly polygon-based spatial queries",
            ),
            (
                "ext-session",
                ExtensionModule::Session,
                true,
                "SQLITE_ENABLE_SESSION",
                "Enable session changeset/patchset recording",
            ),
            (
                "ext-preupdate-hook",
                ExtensionModule::Session,
                true,
                "SQLITE_ENABLE_PREUPDATE_HOOK",
                "Enable pre-update hook required by session extension",
            ),
            (
                "ext-icu",
                ExtensionModule::Icu,
                true,
                "SQLITE_ENABLE_ICU",
                "Enable ICU-based collation and Unicode support",
            ),
            (
                "ext-dbstat-vtab",
                ExtensionModule::Misc,
                true,
                "SQLITE_ENABLE_DBSTAT_VTAB",
                "Enable dbstat virtual table for page-level statistics",
            ),
            (
                "ext-dbpage-vtab",
                ExtensionModule::Misc,
                true,
                "SQLITE_ENABLE_DBPAGE_VTAB",
                "Enable dbpage virtual table for direct page access",
            ),
        ];

        for (name, module, enabled, sqlite_define, description) in entries {
            flags.insert(
                name.to_owned(),
                FeatureFlag {
                    name: name.to_owned(),
                    module,
                    enabled_by_default: enabled,
                    sqlite_define: sqlite_define.to_owned(),
                    description: description.to_owned(),
                },
            );
        }

        Self {
            schema_version: MATRIX_SCHEMA_VERSION,
            flags,
        }
    }
}

// ---------------------------------------------------------------------------
// Surface point (individual function/operator/vtab)
// ---------------------------------------------------------------------------

/// Kind of extension surface entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum SurfaceKind {
    /// A scalar function (e.g., `json()`).
    ScalarFunction,
    /// An aggregate function (e.g., `json_group_array()`).
    AggregateFunction,
    /// A table-valued function (e.g., `json_each()`).
    TableValuedFunction,
    /// A virtual table (e.g., FTS3, R-tree).
    VirtualTable,
    /// An operator (e.g., `->`, `MATCH`).
    Operator,
    /// A PRAGMA or configuration surface.
    Configuration,
    /// An API function (e.g., `sqlite3session_create`).
    ApiFunction,
}

impl fmt::Display for SurfaceKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ScalarFunction => f.write_str("scalar_function"),
            Self::AggregateFunction => f.write_str("aggregate_function"),
            Self::TableValuedFunction => f.write_str("table_valued_function"),
            Self::VirtualTable => f.write_str("virtual_table"),
            Self::Operator => f.write_str("operator"),
            Self::Configuration => f.write_str("configuration"),
            Self::ApiFunction => f.write_str("api_function"),
        }
    }
}

// ---------------------------------------------------------------------------
// Omission rationale
// ---------------------------------------------------------------------------

/// Reason for intentionally omitting an extension surface point.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OmissionRationale {
    /// Why this surface point is omitted.
    pub reason: String,
    /// Upstream SQLite version where this was introduced (if relevant).
    pub introduced_version: Option<String>,
    /// Whether this may be implemented in a future release.
    pub future_candidate: bool,
}

// ---------------------------------------------------------------------------
// Acceptance test reference
// ---------------------------------------------------------------------------

/// A reference to an acceptance test that validates a surface point.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct AcceptanceTestRef {
    /// Test module path (e.g., `fsqlite-ext-json::tests::json_extraction`).
    pub module_path: String,
    /// Specific test function name, if applicable.
    pub test_name: Option<String>,
    /// Whether this test runs as part of the differential oracle suite.
    pub oracle_differential: bool,
}

// ---------------------------------------------------------------------------
// Contract entry
// ---------------------------------------------------------------------------

/// A single surface-point contract in the extension parity matrix.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContractEntry {
    /// Unique surface-point ID (e.g., `"EXT-JSON-001"`).
    pub id: String,
    /// Extension module this belongs to.
    pub module: ExtensionModule,
    /// Kind of surface point.
    pub kind: SurfaceKind,
    /// Human-readable name (e.g., `"json()"`).
    pub name: String,
    /// Expected SQLite 3.52.0 behaviour description.
    pub expected_behavior: String,
    /// Current implementation status.
    pub status: ParityStatus,
    /// Feature flag(s) that must be enabled.
    pub required_flags: Vec<String>,
    /// Link back to the parity taxonomy feature.
    pub taxonomy_feature_id: Option<FeatureId>,
    /// Omission rationale (only when `status` is `Missing` or `Excluded`).
    pub omission: Option<OmissionRationale>,
    /// Acceptance tests that validate this surface point.
    pub acceptance_tests: Vec<AcceptanceTestRef>,
    /// Tags for cross-cutting queries.
    pub tags: BTreeSet<String>,
}

// ---------------------------------------------------------------------------
// Extension parity matrix
// ---------------------------------------------------------------------------

/// The complete extension parity contract matrix.
///
/// Machine-readable structure consumed by CI gates and reporting.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtensionParityMatrix {
    /// Schema version.
    pub schema_version: u32,
    /// Target SQLite version.
    pub target_sqlite_version: String,
    /// Feature flag truth table.
    pub feature_flags: FeatureFlagTable,
    /// Contract entries keyed by surface-point ID.
    pub entries: BTreeMap<String, ContractEntry>,
}

impl ExtensionParityMatrix {
    /// Build the canonical matrix covering all extension surface points.
    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn canonical() -> Self {
        let feature_flags = FeatureFlagTable::canonical();
        let mut entries = BTreeMap::new();

        // Helper to insert an entry and return the ID for chaining.
        let mut seq: u16 = 0;
        let mut add = |module: ExtensionModule,
                       kind: SurfaceKind,
                       name: &str,
                       expected: &str,
                       status: ParityStatus,
                       flags: &[&str],
                       taxonomy_seq: Option<u16>,
                       omission: Option<OmissionRationale>,
                       tags: &[&str]| {
            seq += 1;
            let prefix = match module {
                ExtensionModule::Fts3 => "FTS3",
                ExtensionModule::Fts5 => "FTS5",
                ExtensionModule::Json => "JSON",
                ExtensionModule::Rtree => "RTREE",
                ExtensionModule::Session => "SESS",
                ExtensionModule::Icu => "ICU",
                ExtensionModule::Misc => "MISC",
            };
            let id = format!("EXT-{prefix}-{seq:03}");
            let taxonomy_feature_id = taxonomy_seq.map(|s| FeatureId::new("EXT", s));
            let entry = ContractEntry {
                id: id.clone(),
                module,
                kind,
                name: name.to_owned(),
                expected_behavior: expected.to_owned(),
                status,
                required_flags: flags.iter().map(|&s| s.to_owned()).collect(),
                taxonomy_feature_id,
                omission,
                acceptance_tests: Vec::new(),
                tags: tags.iter().map(|&s| s.to_owned()).collect(),
            };
            entries.insert(id, entry);
        };

        // =================================================================
        // FTS3/FTS4
        // =================================================================
        add(
            ExtensionModule::Fts3,
            SurfaceKind::VirtualTable,
            "FTS3 virtual table",
            "CREATE VIRTUAL TABLE ... USING fts3(...) creates full-text index",
            ParityStatus::Partial,
            &["ext-fts3"],
            Some(1),
            None,
            &["ext", "fts3", "vtab"],
        );
        add(
            ExtensionModule::Fts3,
            SurfaceKind::Operator,
            "FTS3 MATCH operator",
            "SELECT ... WHERE col MATCH 'term' returns matching rows with ranking",
            ParityStatus::Partial,
            &["ext-fts3"],
            Some(1),
            None,
            &["ext", "fts3", "match"],
        );
        add(
            ExtensionModule::Fts3,
            SurfaceKind::ScalarFunction,
            "snippet()",
            "Returns text excerpt with search terms highlighted",
            ParityStatus::Partial,
            &["ext-fts3"],
            Some(1),
            None,
            &["ext", "fts3", "snippet"],
        );
        add(
            ExtensionModule::Fts3,
            SurfaceKind::ScalarFunction,
            "offsets()",
            "Returns byte offsets of matching terms",
            ParityStatus::Partial,
            &["ext-fts3"],
            Some(1),
            None,
            &["ext", "fts3", "offsets"],
        );
        add(
            ExtensionModule::Fts3,
            SurfaceKind::ScalarFunction,
            "matchinfo()",
            "Returns binary match statistics blob",
            ParityStatus::Partial,
            &["ext-fts3"],
            Some(1),
            None,
            &["ext", "fts3", "matchinfo"],
        );
        add(
            ExtensionModule::Fts3,
            SurfaceKind::Configuration,
            "fts3_tokenizer()",
            "Register/retrieve custom tokenizer implementations",
            ParityStatus::Partial,
            &["ext-fts3", "ext-fts3-tokenizer"],
            Some(2),
            None,
            &["ext", "fts3", "tokenizer"],
        );
        add(
            ExtensionModule::Fts3,
            SurfaceKind::Configuration,
            "simple tokenizer",
            "Default whitespace + ASCII case-folding tokenizer",
            ParityStatus::Partial,
            &["ext-fts3"],
            Some(2),
            None,
            &["ext", "fts3", "tokenizer"],
        );
        add(
            ExtensionModule::Fts3,
            SurfaceKind::Configuration,
            "porter tokenizer",
            "Porter stemming tokenizer",
            ParityStatus::Partial,
            &["ext-fts3"],
            Some(2),
            None,
            &["ext", "fts3", "tokenizer"],
        );
        add(
            ExtensionModule::Fts3,
            SurfaceKind::Configuration,
            "unicode61 tokenizer",
            "Unicode 6.1 aware tokenizer with diacritic folding",
            ParityStatus::Partial,
            &["ext-fts3"],
            Some(2),
            None,
            &["ext", "fts3", "tokenizer"],
        );
        // FTS4
        add(
            ExtensionModule::Fts3,
            SurfaceKind::VirtualTable,
            "FTS4 virtual table",
            "CREATE VIRTUAL TABLE ... USING fts4(...) with content= and languageid=",
            ParityStatus::Partial,
            &["ext-fts3", "ext-fts4"],
            Some(3),
            None,
            &["ext", "fts4", "vtab"],
        );
        add(
            ExtensionModule::Fts3,
            SurfaceKind::Configuration,
            "FTS4 prefix= option",
            "Enable prefix search with specified prefix lengths",
            ParityStatus::Partial,
            &["ext-fts3", "ext-fts4"],
            Some(3),
            None,
            &["ext", "fts4", "prefix"],
        );
        add(
            ExtensionModule::Fts3,
            SurfaceKind::Configuration,
            "FTS4 content= option",
            "External content FTS4 table backed by separate table",
            ParityStatus::Partial,
            &["ext-fts3", "ext-fts4"],
            Some(3),
            None,
            &["ext", "fts4", "content"],
        );

        // =================================================================
        // FTS5
        // =================================================================
        // FTS5 entries continue from global seq counter
        add(
            ExtensionModule::Fts5,
            SurfaceKind::VirtualTable,
            "FTS5 virtual table",
            "CREATE VIRTUAL TABLE ... USING fts5(...) with columnsize and detail options",
            ParityStatus::Partial,
            &["ext-fts5"],
            Some(4),
            None,
            &["ext", "fts5", "vtab"],
        );
        add(
            ExtensionModule::Fts5,
            SurfaceKind::Operator,
            "FTS5 MATCH operator",
            "Full-text query with AND/OR/NOT/NEAR/phrase support",
            ParityStatus::Partial,
            &["ext-fts5"],
            Some(4),
            None,
            &["ext", "fts5", "match"],
        );
        add(
            ExtensionModule::Fts5,
            SurfaceKind::ScalarFunction,
            "bm25()",
            "BM25 relevance ranking function",
            ParityStatus::Partial,
            &["ext-fts5"],
            Some(5),
            None,
            &["ext", "fts5", "bm25"],
        );
        add(
            ExtensionModule::Fts5,
            SurfaceKind::ScalarFunction,
            "highlight()",
            "Returns text with matching terms wrapped in markers",
            ParityStatus::Partial,
            &["ext-fts5"],
            Some(5),
            None,
            &["ext", "fts5", "highlight"],
        );
        add(
            ExtensionModule::Fts5,
            SurfaceKind::ScalarFunction,
            "snippet()",
            "Returns text excerpt around matching terms",
            ParityStatus::Partial,
            &["ext-fts5"],
            Some(5),
            None,
            &["ext", "fts5", "snippet"],
        );
        add(
            ExtensionModule::Fts5,
            SurfaceKind::Configuration,
            "FTS5 tokenize= option",
            "Configure tokenizer (unicode61, ascii, porter, trigram)",
            ParityStatus::Partial,
            &["ext-fts5"],
            Some(6),
            None,
            &["ext", "fts5", "tokenizer"],
        );
        add(
            ExtensionModule::Fts5,
            SurfaceKind::ApiFunction,
            "fts5_api custom tokenizers",
            "Register custom FTS5 tokenizers via xCreate/xDelete/xTokenize",
            ParityStatus::Partial,
            &["ext-fts5"],
            Some(6),
            None,
            &["ext", "fts5", "tokenizer", "api"],
        );

        // =================================================================
        // JSON1
        // =================================================================
        add(
            ExtensionModule::Json,
            SurfaceKind::ScalarFunction,
            "json()",
            "Validate and canonicalize JSON text",
            ParityStatus::Passing,
            &["ext-json1"],
            Some(7),
            None,
            &["ext", "json"],
        );
        add(
            ExtensionModule::Json,
            SurfaceKind::ScalarFunction,
            "json_valid()",
            "Return 1 if argument is well-formed JSON, 0 otherwise",
            ParityStatus::Passing,
            &["ext-json1"],
            Some(7),
            None,
            &["ext", "json"],
        );
        add(
            ExtensionModule::Json,
            SurfaceKind::ScalarFunction,
            "json_extract()",
            "Extract value at JSON path",
            ParityStatus::Passing,
            &["ext-json1"],
            Some(8),
            None,
            &["ext", "json", "extract"],
        );
        add(
            ExtensionModule::Json,
            SurfaceKind::ScalarFunction,
            "json_type()",
            "Return JSON type at path (null/true/false/integer/real/text/array/object)",
            ParityStatus::Passing,
            &["ext-json1"],
            Some(8),
            None,
            &["ext", "json", "type"],
        );
        add(
            ExtensionModule::Json,
            SurfaceKind::ScalarFunction,
            "json_set()",
            "Set value at JSON path, creating intermediates",
            ParityStatus::Passing,
            &["ext-json1"],
            Some(9),
            None,
            &["ext", "json", "mutation"],
        );
        add(
            ExtensionModule::Json,
            SurfaceKind::ScalarFunction,
            "json_insert()",
            "Insert value at JSON path only if it does not exist",
            ParityStatus::Passing,
            &["ext-json1"],
            Some(9),
            None,
            &["ext", "json", "mutation"],
        );
        add(
            ExtensionModule::Json,
            SurfaceKind::ScalarFunction,
            "json_replace()",
            "Replace value at JSON path only if it exists",
            ParityStatus::Passing,
            &["ext-json1"],
            Some(9),
            None,
            &["ext", "json", "mutation"],
        );
        add(
            ExtensionModule::Json,
            SurfaceKind::ScalarFunction,
            "json_remove()",
            "Remove element at JSON path",
            ParityStatus::Passing,
            &["ext-json1"],
            Some(9),
            None,
            &["ext", "json", "mutation"],
        );
        add(
            ExtensionModule::Json,
            SurfaceKind::ScalarFunction,
            "json_patch()",
            "Apply RFC 7396 merge patch to JSON document",
            ParityStatus::Passing,
            &["ext-json1"],
            Some(9),
            None,
            &["ext", "json", "mutation"],
        );
        add(
            ExtensionModule::Json,
            SurfaceKind::ScalarFunction,
            "json_array()",
            "Return JSON array from arguments",
            ParityStatus::Passing,
            &["ext-json1"],
            Some(7),
            None,
            &["ext", "json"],
        );
        add(
            ExtensionModule::Json,
            SurfaceKind::ScalarFunction,
            "json_object()",
            "Return JSON object from key/value argument pairs",
            ParityStatus::Passing,
            &["ext-json1"],
            Some(7),
            None,
            &["ext", "json"],
        );
        add(
            ExtensionModule::Json,
            SurfaceKind::ScalarFunction,
            "json_quote()",
            "Quote a SQL value as JSON",
            ParityStatus::Passing,
            &["ext-json1"],
            Some(7),
            None,
            &["ext", "json"],
        );
        add(
            ExtensionModule::Json,
            SurfaceKind::TableValuedFunction,
            "json_each()",
            "Table-valued function decomposing JSON array/object into rows",
            ParityStatus::Passing,
            &["ext-json1"],
            Some(10),
            None,
            &["ext", "json", "tvf"],
        );
        add(
            ExtensionModule::Json,
            SurfaceKind::TableValuedFunction,
            "json_tree()",
            "Recursive table-valued decomposition of nested JSON",
            ParityStatus::Passing,
            &["ext-json1"],
            Some(10),
            None,
            &["ext", "json", "tvf"],
        );
        add(
            ExtensionModule::Json,
            SurfaceKind::AggregateFunction,
            "json_group_array()",
            "Aggregate values into JSON array",
            ParityStatus::Passing,
            &["ext-json1"],
            Some(11),
            None,
            &["ext", "json", "agg"],
        );
        add(
            ExtensionModule::Json,
            SurfaceKind::AggregateFunction,
            "json_group_object()",
            "Aggregate key/value pairs into JSON object",
            ParityStatus::Passing,
            &["ext-json1"],
            Some(11),
            None,
            &["ext", "json", "agg"],
        );
        add(
            ExtensionModule::Json,
            SurfaceKind::Operator,
            "-> operator",
            "JSON extraction returning JSON text (3.38+)",
            ParityStatus::Missing,
            &["ext-json1"],
            Some(12),
            Some(OmissionRationale {
                reason: "Arrow operators require parser changes for infix JSON \
                         extraction; planned for a future closure wave"
                    .to_owned(),
                introduced_version: Some("3.38.0".to_owned()),
                future_candidate: true,
            }),
            &["ext", "json", "operator"],
        );
        add(
            ExtensionModule::Json,
            SurfaceKind::Operator,
            "->> operator",
            "JSON extraction returning SQL value (3.38+)",
            ParityStatus::Missing,
            &["ext-json1"],
            Some(12),
            Some(OmissionRationale {
                reason: "Arrow operators require parser changes for infix JSON \
                         extraction; planned for a future closure wave"
                    .to_owned(),
                introduced_version: Some("3.38.0".to_owned()),
                future_candidate: true,
            }),
            &["ext", "json", "operator"],
        );

        // =================================================================
        // R-tree
        // =================================================================
        add(
            ExtensionModule::Rtree,
            SurfaceKind::VirtualTable,
            "rtree virtual table",
            "CREATE VIRTUAL TABLE ... USING rtree(id, x0, x1, y0, y1, ...)",
            ParityStatus::Partial,
            &["ext-rtree"],
            Some(13),
            None,
            &["ext", "rtree", "vtab"],
        );
        add(
            ExtensionModule::Rtree,
            SurfaceKind::Operator,
            "R-tree range query",
            "SELECT ... WHERE x0 >= ? AND x1 <= ? uses spatial index",
            ParityStatus::Partial,
            &["ext-rtree"],
            Some(14),
            None,
            &["ext", "rtree", "query"],
        );
        add(
            ExtensionModule::Rtree,
            SurfaceKind::Operator,
            "R-tree containment query",
            "SELECT ... WHERE id MATCH rtreecheck() geometry callbacks",
            ParityStatus::Partial,
            &["ext-rtree"],
            Some(14),
            None,
            &["ext", "rtree", "query"],
        );
        add(
            ExtensionModule::Rtree,
            SurfaceKind::VirtualTable,
            "Geopoly virtual table",
            "CREATE VIRTUAL TABLE ... USING geopoly(...) polygon spatial index",
            ParityStatus::Missing,
            &["ext-rtree", "ext-geopoly"],
            Some(15),
            Some(OmissionRationale {
                reason: "Geopoly is a relatively new extension with limited adoption; \
                         not required for core parity"
                    .to_owned(),
                introduced_version: Some("3.25.0".to_owned()),
                future_candidate: true,
            }),
            &["ext", "rtree", "geopoly"],
        );
        add(
            ExtensionModule::Rtree,
            SurfaceKind::ScalarFunction,
            "geopoly_overlap()",
            "Test whether two polygons overlap",
            ParityStatus::Missing,
            &["ext-rtree", "ext-geopoly"],
            Some(15),
            Some(OmissionRationale {
                reason: "Dependent on Geopoly virtual table implementation".to_owned(),
                introduced_version: Some("3.25.0".to_owned()),
                future_candidate: true,
            }),
            &["ext", "rtree", "geopoly"],
        );
        add(
            ExtensionModule::Rtree,
            SurfaceKind::ScalarFunction,
            "geopoly_within()",
            "Test whether one polygon is within another",
            ParityStatus::Missing,
            &["ext-rtree", "ext-geopoly"],
            Some(15),
            Some(OmissionRationale {
                reason: "Dependent on Geopoly virtual table implementation".to_owned(),
                introduced_version: Some("3.25.0".to_owned()),
                future_candidate: true,
            }),
            &["ext", "rtree", "geopoly"],
        );

        // =================================================================
        // Session
        // =================================================================
        add(
            ExtensionModule::Session,
            SurfaceKind::ApiFunction,
            "sqlite3session_create()",
            "Create a session object attached to a database connection",
            ParityStatus::Partial,
            &["ext-session"],
            Some(16),
            None,
            &["ext", "session", "api"],
        );
        add(
            ExtensionModule::Session,
            SurfaceKind::ApiFunction,
            "sqlite3session_attach()",
            "Attach a table to the session for change recording",
            ParityStatus::Partial,
            &["ext-session"],
            Some(16),
            None,
            &["ext", "session", "api"],
        );
        add(
            ExtensionModule::Session,
            SurfaceKind::ApiFunction,
            "sqlite3session_changeset()",
            "Generate changeset blob from recorded changes",
            ParityStatus::Partial,
            &["ext-session"],
            Some(16),
            None,
            &["ext", "session", "changeset"],
        );
        add(
            ExtensionModule::Session,
            SurfaceKind::ApiFunction,
            "sqlite3changeset_apply()",
            "Apply changeset blob to a database",
            ParityStatus::Partial,
            &["ext-session"],
            Some(16),
            None,
            &["ext", "session", "changeset"],
        );
        add(
            ExtensionModule::Session,
            SurfaceKind::ApiFunction,
            "sqlite3session_patchset()",
            "Generate compact patchset blob",
            ParityStatus::Partial,
            &["ext-session"],
            Some(17),
            None,
            &["ext", "session", "patchset"],
        );
        add(
            ExtensionModule::Session,
            SurfaceKind::ApiFunction,
            "sqlite3changeset_conflict()",
            "Conflict resolution callback during changeset apply",
            ParityStatus::Partial,
            &["ext-session", "ext-preupdate-hook"],
            Some(18),
            None,
            &["ext", "session", "conflict"],
        );
        add(
            ExtensionModule::Session,
            SurfaceKind::ApiFunction,
            "sqlite3changeset_invert()",
            "Invert a changeset for rollback",
            ParityStatus::Partial,
            &["ext-session"],
            Some(16),
            None,
            &["ext", "session", "changeset"],
        );
        add(
            ExtensionModule::Session,
            SurfaceKind::ApiFunction,
            "sqlite3changeset_concat()",
            "Concatenate two changesets",
            ParityStatus::Partial,
            &["ext-session"],
            Some(16),
            None,
            &["ext", "session", "changeset"],
        );

        // =================================================================
        // ICU
        // =================================================================
        add(
            ExtensionModule::Icu,
            SurfaceKind::Configuration,
            "ICU collation registration",
            "Register locale-aware collation sequences via icu_load_collation()",
            ParityStatus::Partial,
            &["ext-icu"],
            Some(19),
            None,
            &["ext", "icu", "collation"],
        );
        add(
            ExtensionModule::Icu,
            SurfaceKind::ScalarFunction,
            "icu_load_collation()",
            "Load ICU collation by locale name",
            ParityStatus::Partial,
            &["ext-icu"],
            Some(19),
            None,
            &["ext", "icu", "collation"],
        );
        add(
            ExtensionModule::Icu,
            SurfaceKind::Operator,
            "ICU LIKE operator",
            "Unicode-aware LIKE with ICU case folding",
            ParityStatus::Missing,
            &["ext-icu"],
            Some(20),
            Some(OmissionRationale {
                reason: "ICU LIKE/REGEXP requires integration with ICU library \
                         for Unicode case folding beyond ASCII; planned for future wave"
                    .to_owned(),
                introduced_version: None,
                future_candidate: true,
            }),
            &["ext", "icu", "like"],
        );
        add(
            ExtensionModule::Icu,
            SurfaceKind::ScalarFunction,
            "icu_regexp()",
            "Unicode-aware regular expression matching",
            ParityStatus::Missing,
            &["ext-icu"],
            Some(20),
            Some(OmissionRationale {
                reason: "ICU REGEXP requires ICU regex engine integration; \
                         planned for future wave"
                    .to_owned(),
                introduced_version: None,
                future_candidate: true,
            }),
            &["ext", "icu", "regexp"],
        );

        // =================================================================
        // Misc
        // =================================================================
        add(
            ExtensionModule::Misc,
            SurfaceKind::TableValuedFunction,
            "generate_series()",
            "Generate integer sequence: generate_series(start, stop, step)",
            ParityStatus::Passing,
            &[],
            Some(21),
            None,
            &["ext", "misc", "tvf"],
        );
        add(
            ExtensionModule::Misc,
            SurfaceKind::VirtualTable,
            "dbstat virtual table",
            "SELECT * FROM dbstat reports per-page statistics",
            ParityStatus::Partial,
            &["ext-dbstat-vtab"],
            Some(22),
            None,
            &["ext", "misc", "vtab"],
        );
        add(
            ExtensionModule::Misc,
            SurfaceKind::VirtualTable,
            "dbpage virtual table",
            "SELECT/UPDATE dbpage for direct page-level access",
            ParityStatus::Partial,
            &["ext-dbpage-vtab"],
            Some(23),
            None,
            &["ext", "misc", "vtab"],
        );
        add(
            ExtensionModule::Misc,
            SurfaceKind::TableValuedFunction,
            "carray()",
            "Bind C-array pointer as virtual table rows",
            ParityStatus::Missing,
            &[],
            Some(24),
            Some(OmissionRationale {
                reason: "carray() requires C pointer binding semantics incompatible \
                         with safe Rust; replaced by native Rust slice binding API"
                    .to_owned(),
                introduced_version: None,
                future_candidate: false,
            }),
            &["ext", "misc", "tvf"],
        );
        add(
            ExtensionModule::Misc,
            SurfaceKind::ScalarFunction,
            "decimal_sum()",
            "Arbitrary-precision decimal aggregation",
            ParityStatus::Partial,
            &[],
            None,
            None,
            &["ext", "misc", "decimal"],
        );
        add(
            ExtensionModule::Misc,
            SurfaceKind::ScalarFunction,
            "uuid()",
            "Generate RFC 4122 UUID v4",
            ParityStatus::Partial,
            &[],
            None,
            None,
            &["ext", "misc", "uuid"],
        );

        Self {
            schema_version: MATRIX_SCHEMA_VERSION,
            target_sqlite_version: "3.52.0".to_owned(),
            feature_flags,
            entries,
        }
    }

    /// Validate internal consistency of the matrix.
    ///
    /// Returns a list of diagnostic messages (empty = valid).
    #[must_use]
    pub fn validate(&self) -> Vec<String> {
        let mut diagnostics = Vec::new();

        // Check unique IDs (guaranteed by BTreeMap keys, but verify entries match).
        for (key, entry) in &self.entries {
            if key != &entry.id {
                diagnostics.push(format!("Key/ID mismatch: key={key} entry.id={}", entry.id));
            }
        }

        // Check that required flags reference known flags.
        let known_flags: BTreeSet<&str> = self
            .feature_flags
            .flags
            .keys()
            .map(String::as_str)
            .collect();
        for entry in self.entries.values() {
            for flag in &entry.required_flags {
                if !known_flags.contains(flag.as_str()) {
                    diagnostics.push(format!(
                        "Entry {} references unknown flag: {flag}",
                        entry.id
                    ));
                }
            }
        }

        // Check omission rationale consistency.
        for entry in self.entries.values() {
            let needs_omission =
                entry.status == ParityStatus::Missing || entry.status == ParityStatus::Excluded;
            if needs_omission && entry.omission.is_none() {
                // Missing entries without omission rationale are acceptable
                // (they may just not be implemented yet without a specific reason).
            }
            if !needs_omission && entry.omission.is_some() {
                diagnostics.push(format!(
                    "Entry {} has omission rationale but status is {:?}",
                    entry.id, entry.status
                ));
            }
        }

        diagnostics
    }

    /// Count entries by module.
    #[must_use]
    pub fn count_by_module(&self) -> BTreeMap<ExtensionModule, usize> {
        let mut counts = BTreeMap::new();
        for entry in self.entries.values() {
            *counts.entry(entry.module).or_insert(0) += 1;
        }
        counts
    }

    /// Count entries by status.
    #[must_use]
    pub fn count_by_status(&self) -> BTreeMap<String, usize> {
        let mut counts = BTreeMap::new();
        for entry in self.entries.values() {
            *counts.entry(format!("{}", entry.status)).or_insert(0) += 1;
        }
        counts
    }

    /// Get all entries for a specific module.
    #[must_use]
    pub fn entries_for_module(&self, module: ExtensionModule) -> Vec<&ContractEntry> {
        self.entries
            .values()
            .filter(|e| e.module == module)
            .collect()
    }

    /// Get all intentionally omitted surface points.
    #[must_use]
    pub fn intentional_omissions(&self) -> Vec<&ContractEntry> {
        self.entries
            .values()
            .filter(|e| e.omission.is_some())
            .collect()
    }

    /// Get surface points that are candidates for future implementation.
    #[must_use]
    pub fn future_candidates(&self) -> Vec<&ContractEntry> {
        self.entries
            .values()
            .filter(|e| e.omission.as_ref().is_some_and(|o| o.future_candidate))
            .collect()
    }

    /// Get entries matching any of the given tags.
    #[must_use]
    pub fn entries_by_tags(&self, tags: &[&str]) -> Vec<&ContractEntry> {
        let tag_set: BTreeSet<&str> = tags.iter().copied().collect();
        self.entries
            .values()
            .filter(|e| e.tags.iter().any(|t| tag_set.contains(t.as_str())))
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Coverage summary
// ---------------------------------------------------------------------------

/// Per-module coverage summary.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModuleCoverage {
    /// Extension module.
    pub module: ExtensionModule,
    /// Total surface points.
    pub total: usize,
    /// Passing surface points.
    pub passing: usize,
    /// Partial surface points.
    pub partial: usize,
    /// Missing surface points.
    pub missing: usize,
    /// Excluded surface points.
    pub excluded: usize,
    /// Coverage ratio (passing + 0.5*partial) / (total - excluded).
    pub coverage_ratio: f64,
}

/// Overall extension parity coverage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtensionCoverage {
    /// Schema version.
    pub schema_version: u32,
    /// Per-module coverage.
    pub modules: Vec<ModuleCoverage>,
    /// Aggregate surface point count.
    pub total_surface_points: usize,
    /// Aggregate passing count.
    pub total_passing: usize,
    /// Aggregate missing count.
    pub total_missing: usize,
    /// Overall coverage ratio.
    pub overall_coverage_ratio: f64,
    /// Number of intentional omissions.
    pub intentional_omissions: usize,
    /// Number of future implementation candidates.
    pub future_candidates: usize,
}

/// Truncate to 6 decimal places for deterministic scoring.
fn truncate_6(val: f64) -> f64 {
    (val * 1_000_000.0).trunc() / 1_000_000.0
}

/// Compute extension parity coverage from a matrix.
#[must_use]
pub fn compute_extension_coverage(matrix: &ExtensionParityMatrix) -> ExtensionCoverage {
    let mut modules = Vec::new();
    let mut total_all = 0_usize;
    let mut passing_all = 0_usize;
    let mut missing_all = 0_usize;
    let mut numerator_all = 0.0_f64;
    let mut denominator_all = 0_usize;

    for module in ExtensionModule::ALL {
        let entries = matrix.entries_for_module(module);
        let total = entries.len();
        let passing = entries
            .iter()
            .filter(|e| e.status == ParityStatus::Passing)
            .count();
        let partial = entries
            .iter()
            .filter(|e| e.status == ParityStatus::Partial)
            .count();
        let missing = entries
            .iter()
            .filter(|e| e.status == ParityStatus::Missing)
            .count();
        let excluded = entries
            .iter()
            .filter(|e| e.status == ParityStatus::Excluded)
            .count();

        let denom = total - excluded;
        #[allow(clippy::cast_precision_loss)]
        let coverage_ratio = if denom > 0 {
            truncate_6(0.5f64.mul_add(partial as f64, passing as f64) / denom as f64)
        } else {
            0.0
        };

        total_all += total;
        passing_all += passing;
        missing_all += missing;
        #[allow(clippy::cast_precision_loss)]
        {
            numerator_all += 0.5f64.mul_add(partial as f64, passing as f64);
        }
        denominator_all += denom;

        modules.push(ModuleCoverage {
            module,
            total,
            passing,
            partial,
            missing,
            excluded,
            coverage_ratio,
        });
    }

    #[allow(clippy::cast_precision_loss)]
    let overall = if denominator_all > 0 {
        truncate_6(numerator_all / denominator_all as f64)
    } else {
        0.0
    };

    ExtensionCoverage {
        schema_version: MATRIX_SCHEMA_VERSION,
        modules,
        total_surface_points: total_all,
        total_passing: passing_all,
        total_missing: missing_all,
        overall_coverage_ratio: overall,
        intentional_omissions: matrix.intentional_omissions().len(),
        future_candidates: matrix.future_candidates().len(),
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_matrix_validates() {
        let matrix = ExtensionParityMatrix::canonical();
        let diagnostics = matrix.validate();
        assert!(diagnostics.is_empty(), "Validation failed: {diagnostics:?}");
    }

    #[test]
    fn canonical_matrix_has_all_modules() {
        let matrix = ExtensionParityMatrix::canonical();
        let counts = matrix.count_by_module();
        for module in ExtensionModule::ALL {
            assert!(counts.contains_key(&module), "Missing module: {module:?}");
            assert!(
                counts[&module] > 0,
                "Module {module:?} has zero surface points"
            );
        }
    }

    #[test]
    fn canonical_matrix_surface_point_count() {
        let matrix = ExtensionParityMatrix::canonical();
        // We expect a reasonable number of surface points across all modules.
        assert!(
            matrix.entries.len() >= 40,
            "Expected >= 40 surface points, got {}",
            matrix.entries.len()
        );
    }

    #[test]
    fn feature_flag_table_covers_all_modules() {
        let table = FeatureFlagTable::canonical();
        let modules_with_flags: BTreeSet<ExtensionModule> =
            table.flags.values().map(|f| f.module).collect();
        // Every module except Misc (which has some flag-free entries) should have flags.
        for module in [
            ExtensionModule::Fts3,
            ExtensionModule::Fts5,
            ExtensionModule::Json,
            ExtensionModule::Rtree,
            ExtensionModule::Session,
            ExtensionModule::Icu,
            ExtensionModule::Misc,
        ] {
            assert!(
                modules_with_flags.contains(&module),
                "Module {module:?} has no feature flags"
            );
        }
    }

    #[test]
    fn feature_flags_have_unique_names() {
        let table = FeatureFlagTable::canonical();
        let count = table.flags.len();
        let unique_names: BTreeSet<&str> = table.flags.keys().map(String::as_str).collect();
        assert_eq!(count, unique_names.len(), "Duplicate flag names detected");
    }

    #[test]
    fn json_functions_are_mostly_passing() {
        let matrix = ExtensionParityMatrix::canonical();
        let json_entries = matrix.entries_for_module(ExtensionModule::Json);
        let passing = json_entries
            .iter()
            .filter(|e| e.status == ParityStatus::Passing)
            .count();
        // JSON1 core functions should be mostly passing.
        assert!(
            passing >= 10,
            "Expected >= 10 passing JSON entries, got {passing}"
        );
    }

    #[test]
    fn arrow_operators_are_missing_with_rationale() {
        let matrix = ExtensionParityMatrix::canonical();
        let arrows: Vec<_> = matrix
            .entries
            .values()
            .filter(|e| e.name == "-> operator" || e.name == "->> operator")
            .collect();
        assert_eq!(arrows.len(), 2, "Expected 2 arrow operator entries");
        for entry in &arrows {
            assert_eq!(entry.status, ParityStatus::Missing);
            assert!(
                entry.omission.is_some(),
                "Arrow operator {} lacks omission rationale",
                entry.name
            );
            let omission = entry.omission.as_ref().unwrap();
            assert!(omission.future_candidate);
        }
    }

    #[test]
    fn geopoly_is_missing_with_rationale() {
        let matrix = ExtensionParityMatrix::canonical();
        let geopoly: Vec<_> = matrix
            .entries
            .values()
            .filter(|e| e.tags.contains("geopoly"))
            .collect();
        assert!(
            geopoly.len() >= 2,
            "Expected >= 2 geopoly entries, got {}",
            geopoly.len()
        );
        for entry in &geopoly {
            assert_eq!(
                entry.status,
                ParityStatus::Missing,
                "Geopoly entry {} should be missing",
                entry.name
            );
            assert!(entry.omission.is_some());
        }
    }

    #[test]
    fn carray_is_missing_not_future_candidate() {
        let matrix = ExtensionParityMatrix::canonical();
        let carray: Vec<_> = matrix
            .entries
            .values()
            .filter(|e| e.name == "carray()")
            .collect();
        assert_eq!(carray.len(), 1);
        let entry = carray[0];
        assert_eq!(entry.status, ParityStatus::Missing);
        let omission = entry.omission.as_ref().expect("carray needs omission");
        assert!(
            !omission.future_candidate,
            "carray() should not be a future candidate (unsafe Rust)"
        );
    }

    #[test]
    fn icu_like_regexp_missing_with_rationale() {
        let matrix = ExtensionParityMatrix::canonical();
        let icu_missing: Vec<_> = matrix
            .entries_for_module(ExtensionModule::Icu)
            .into_iter()
            .filter(|e| e.status == ParityStatus::Missing)
            .collect();
        assert!(icu_missing.len() >= 2, "Expected >= 2 missing ICU entries");
        for entry in &icu_missing {
            assert!(
                entry.omission.is_some(),
                "Missing ICU entry {} lacks omission rationale",
                entry.name
            );
        }
    }

    #[test]
    fn intentional_omissions_have_rationale() {
        let matrix = ExtensionParityMatrix::canonical();
        let omissions = matrix.intentional_omissions();
        assert!(
            omissions.len() >= 5,
            "Expected >= 5 intentional omissions, got {}",
            omissions.len()
        );
        for entry in &omissions {
            let omission = entry.omission.as_ref().unwrap();
            assert!(
                !omission.reason.is_empty(),
                "Omission for {} has empty reason",
                entry.name
            );
        }
    }

    #[test]
    fn future_candidates_are_subset_of_omissions() {
        let matrix = ExtensionParityMatrix::canonical();
        let candidates = matrix.future_candidates();
        let omissions = matrix.intentional_omissions();
        for candidate in &candidates {
            assert!(
                omissions.iter().any(|o| o.id == candidate.id),
                "Future candidate {} is not in omissions list",
                candidate.id
            );
        }
    }

    #[test]
    fn coverage_computation_is_deterministic() {
        let matrix = ExtensionParityMatrix::canonical();
        let cov1 = compute_extension_coverage(&matrix);
        let cov2 = compute_extension_coverage(&matrix);
        assert_eq!(cov1.overall_coverage_ratio, cov2.overall_coverage_ratio);
        assert_eq!(cov1.total_surface_points, cov2.total_surface_points);
    }

    #[test]
    fn coverage_ratios_are_bounded() {
        let matrix = ExtensionParityMatrix::canonical();
        let coverage = compute_extension_coverage(&matrix);
        assert!(
            (0.0..=1.0).contains(&coverage.overall_coverage_ratio),
            "Overall coverage out of bounds: {}",
            coverage.overall_coverage_ratio
        );
        for module in &coverage.modules {
            assert!(
                (0.0..=1.0).contains(&module.coverage_ratio),
                "Module {:?} coverage out of bounds: {}",
                module.module,
                module.coverage_ratio
            );
        }
    }

    #[test]
    fn coverage_totals_are_consistent() {
        let matrix = ExtensionParityMatrix::canonical();
        let coverage = compute_extension_coverage(&matrix);
        let sum_total: usize = coverage.modules.iter().map(|m| m.total).sum();
        assert_eq!(sum_total, coverage.total_surface_points);
        let sum_passing: usize = coverage.modules.iter().map(|m| m.passing).sum();
        assert_eq!(sum_passing, coverage.total_passing);
        let sum_missing: usize = coverage.modules.iter().map(|m| m.missing).sum();
        assert_eq!(sum_missing, coverage.total_missing);
    }

    #[test]
    fn json_module_has_highest_coverage() {
        let matrix = ExtensionParityMatrix::canonical();
        let coverage = compute_extension_coverage(&matrix);
        let json_cov = coverage
            .modules
            .iter()
            .find(|m| m.module == ExtensionModule::Json)
            .expect("JSON module missing from coverage");
        // JSON is mostly passing, should have highest coverage.
        assert!(
            json_cov.coverage_ratio >= 0.7,
            "JSON coverage unexpectedly low: {}",
            json_cov.coverage_ratio
        );
    }

    #[test]
    fn entries_by_tags_works() {
        let matrix = ExtensionParityMatrix::canonical();
        let fts3_entries = matrix.entries_by_tags(&["fts3"]);
        assert!(!fts3_entries.is_empty(), "No entries found with tag 'fts3'");
        for entry in &fts3_entries {
            assert!(
                entry.tags.contains("fts3"),
                "Entry {} does not have tag 'fts3'",
                entry.id
            );
        }
    }

    #[test]
    fn entries_for_module_filters_correctly() {
        let matrix = ExtensionParityMatrix::canonical();
        for module in ExtensionModule::ALL {
            let entries = matrix.entries_for_module(module);
            for entry in &entries {
                assert_eq!(
                    entry.module, module,
                    "Entry {} has wrong module: {:?}",
                    entry.id, entry.module
                );
            }
        }
    }

    #[test]
    fn count_by_status_covers_all_entries() {
        let matrix = ExtensionParityMatrix::canonical();
        let status_counts = matrix.count_by_status();
        let total: usize = status_counts.values().sum();
        assert_eq!(
            total,
            matrix.entries.len(),
            "Status counts don't sum to total"
        );
    }

    #[test]
    fn extension_module_display_names() {
        for module in ExtensionModule::ALL {
            let name = module.display_name();
            assert!(!name.is_empty(), "Empty display name for {module:?}");
            let crate_name = module.crate_name();
            assert!(
                crate_name.starts_with("fsqlite-ext-"),
                "Crate name doesn't start with fsqlite-ext-: {crate_name}"
            );
        }
    }

    #[test]
    fn surface_kind_display() {
        let kinds = [
            SurfaceKind::ScalarFunction,
            SurfaceKind::AggregateFunction,
            SurfaceKind::TableValuedFunction,
            SurfaceKind::VirtualTable,
            SurfaceKind::Operator,
            SurfaceKind::Configuration,
            SurfaceKind::ApiFunction,
        ];
        for kind in kinds {
            let s = format!("{kind}");
            assert!(!s.is_empty(), "Empty display for {kind:?}");
        }
    }

    #[test]
    fn json_round_trip() {
        let matrix = ExtensionParityMatrix::canonical();
        let json = serde_json::to_string_pretty(&matrix).expect("serialize");
        let restored: ExtensionParityMatrix = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(matrix.entries.len(), restored.entries.len());
        assert_eq!(
            matrix.feature_flags.flags.len(),
            restored.feature_flags.flags.len()
        );
        assert_eq!(matrix.target_sqlite_version, restored.target_sqlite_version);
    }

    #[test]
    fn coverage_json_round_trip() {
        let matrix = ExtensionParityMatrix::canonical();
        let coverage = compute_extension_coverage(&matrix);
        let json = serde_json::to_string_pretty(&coverage).expect("serialize");
        let restored: ExtensionCoverage = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(
            coverage.overall_coverage_ratio,
            restored.overall_coverage_ratio
        );
        assert_eq!(coverage.total_surface_points, restored.total_surface_points);
    }

    #[test]
    fn session_entries_require_session_flag() {
        let matrix = ExtensionParityMatrix::canonical();
        let session_entries = matrix.entries_for_module(ExtensionModule::Session);
        for entry in &session_entries {
            assert!(
                entry.required_flags.contains(&"ext-session".to_owned()),
                "Session entry {} missing ext-session flag",
                entry.name
            );
        }
    }

    #[test]
    fn all_entries_have_nonempty_expected_behavior() {
        let matrix = ExtensionParityMatrix::canonical();
        for entry in matrix.entries.values() {
            assert!(
                !entry.expected_behavior.is_empty(),
                "Entry {} has empty expected_behavior",
                entry.id
            );
        }
    }

    #[test]
    fn fts3_and_fts5_are_separate_modules() {
        let matrix = ExtensionParityMatrix::canonical();
        let fts3 = matrix.entries_for_module(ExtensionModule::Fts3);
        let fts5 = matrix.entries_for_module(ExtensionModule::Fts5);
        assert!(!fts3.is_empty(), "No FTS3 entries");
        assert!(!fts5.is_empty(), "No FTS5 entries");
        // No overlap in IDs.
        let fts3_ids: BTreeSet<&str> = fts3.iter().map(|e| e.id.as_str()).collect();
        let fts5_ids: BTreeSet<&str> = fts5.iter().map(|e| e.id.as_str()).collect();
        let overlap: Vec<_> = fts3_ids.intersection(&fts5_ids).collect();
        assert!(overlap.is_empty(), "FTS3/FTS5 ID overlap: {overlap:?}");
    }

    #[test]
    fn generate_series_is_passing() {
        let matrix = ExtensionParityMatrix::canonical();
        let gs: Vec<_> = matrix
            .entries
            .values()
            .filter(|e| e.name == "generate_series()")
            .collect();
        assert_eq!(gs.len(), 1);
        assert_eq!(gs[0].status, ParityStatus::Passing);
    }
}
