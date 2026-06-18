//! Canonical built-in function parity matrix for bd-2yqp6.5.1.
//!
//! This module provides a machine-readable contract that links the declared
//! built-in-function parity surface from [`crate::parity_taxonomy`] to the
//! concrete differential/unit evidence that currently validates each function
//! family. The matrix is intentionally feature-centric:
//!
//! - every built-in taxonomy feature must appear at least once,
//! - every supported variant must point at explicit verification evidence,
//! - unsupported variants must carry rationale,
//! - known rusqlite binding differences are documented separately from real
//!   parity gaps.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::parity_taxonomy::{FeatureCategory, FeatureId, ParityStatus, build_canonical_universe};

/// Bead identifier for log correlation and evidence indexing.
pub const BEAD_ID: &str = "bd-2yqp6.5.1";

/// Schema version for forward-compatible consumers.
pub const MATRIX_SCHEMA_VERSION: u32 = 1;

const TARGET_SQLITE_VERSION: &str = fsqlite_types::FRANKENSQLITE_SQLITE_VERSION;

/// High-level built-in function family for grouping and reporting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum FunctionFamily {
    /// Scalar functions, including planner hints.
    Scalar,
    /// Aggregate functions.
    Aggregate,
    /// Window functions.
    Window,
    /// Date/time functions.
    Datetime,
    /// Math functions.
    Math,
    /// Stateful/meta helpers exposed as functions.
    Meta,
}

impl FunctionFamily {
    /// Stable display label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Scalar => "scalar",
            Self::Aggregate => "aggregate",
            Self::Window => "window",
            Self::Datetime => "datetime",
            Self::Math => "math",
            Self::Meta => "meta",
        }
    }
}

/// Primary verification strategy for an evidence link.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum VerificationOracle {
    /// Direct oracle comparison against rusqlite / C SQLite behaviour.
    RusqliteDifferential,
    /// Verification against mathematically-known expected values.
    MathematicalOracle,
    /// Engine-level tests executing SQL through `fsqlite-core::Connection`.
    EngineUnit,
    /// Library-level unit tests on built-in implementation modules.
    LibraryUnit,
    /// Metadata/state assertions that do not compare full SQL output sets.
    MetadataAssertion,
}

impl VerificationOracle {
    /// Stable display label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RusqliteDifferential => "rusqlite-differential",
            Self::MathematicalOracle => "mathematical-oracle",
            Self::EngineUnit => "engine-unit",
            Self::LibraryUnit => "library-unit",
            Self::MetadataAssertion => "metadata-assertion",
        }
    }
}

/// Verification result currently recorded for a function variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum VerificationStatus {
    /// Evidence currently passes.
    Passing,
    /// Evidence passes except for documented rusqlite binding artefacts.
    KnownBindingDifference,
    /// Differential evidence currently fails and the parity gap is documented.
    Failing,
    /// Variant is intentionally unsupported.
    Unsupported,
}

impl VerificationStatus {
    /// Stable display label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Passing => "passing",
            Self::KnownBindingDifference => "known-binding-difference",
            Self::Failing => "failing",
            Self::Unsupported => "unsupported",
        }
    }
}

/// Link to a concrete verification artifact.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct CoverageLink {
    /// Relative workspace path to the test file.
    pub test_file: String,
    /// Concrete test function name.
    pub test_name: String,
    /// Primary verification strategy used by that test.
    pub oracle: VerificationOracle,
}

/// Machine-readable row for a built-in function feature/variant pair.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuiltinFunctionVariant {
    /// Stable row ID (`BFUNC-###`).
    pub id: String,
    /// Canonical feature identifier from the parity taxonomy.
    pub feature_id: FeatureId,
    /// Human-readable feature title from the parity taxonomy.
    pub feature_title: String,
    /// Declared feature parity status from the taxonomy.
    pub declared_status: ParityStatus,
    /// Function family.
    pub family: FunctionFamily,
    /// Concrete variant label within the feature family.
    pub variant_name: String,
    /// Current verification result.
    pub verification_status: VerificationStatus,
    /// Representative SQL statements or probes for this variant.
    pub representative_sql: Vec<String>,
    /// Concrete evidence links.
    pub coverage_links: Vec<CoverageLink>,
    /// Human-readable notes for reviewers.
    pub notes: String,
    /// Rationale for unsupported or otherwise special variants.
    pub rationale: Option<String>,
    /// Cross-cutting tags.
    pub tags: BTreeSet<String>,
}

/// Deterministic coverage summary for reporting.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MatrixSummary {
    /// Total number of rows in the matrix.
    pub total_variants: usize,
    /// Number of distinct taxonomy features represented.
    pub total_features: usize,
    /// Counts grouped by family.
    pub variants_by_family: BTreeMap<String, usize>,
    /// Counts grouped by verification status.
    pub variants_by_status: BTreeMap<String, usize>,
}

/// The canonical built-in function parity matrix.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuiltinFunctionParityMatrix {
    /// Schema version.
    pub schema_version: u32,
    /// Bead identifier.
    pub bead_id: String,
    /// Target SQLite version.
    pub target_sqlite_version: String,
    /// Rows keyed by stable row ID for deterministic iteration.
    pub variants: BTreeMap<String, BuiltinFunctionVariant>,
}

impl BuiltinFunctionParityMatrix {
    /// Build the canonical matrix.
    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn canonical() -> Self {
        let feature_catalog = builtin_feature_catalog();
        let mut variants = BTreeMap::new();
        let mut seq: u16 = 0;

        let mut add = |family: FunctionFamily,
                       feature_title: &str,
                       variant_name: &str,
                       verification_status: VerificationStatus,
                       representative_sql: &[&str],
                       coverage_links_seed: &[(&str, &str, VerificationOracle)],
                       notes: &str,
                       rationale: Option<&str>,
                       tags: &[&str]| {
            let (feature_id, declared_status) = feature_catalog
                .get(feature_title)
                .unwrap_or_else(|| panic!("unknown built-in feature title: {feature_title}"))
                .clone();
            seq += 1;
            let id = format!("BFUNC-{seq:03}");
            variants.insert(
                id.clone(),
                BuiltinFunctionVariant {
                    id,
                    feature_id,
                    feature_title: feature_title.to_owned(),
                    declared_status,
                    family,
                    variant_name: variant_name.to_owned(),
                    verification_status,
                    representative_sql: representative_sql
                        .iter()
                        .map(|sql| (*sql).to_owned())
                        .collect(),
                    coverage_links: coverage_links(coverage_links_seed),
                    notes: notes.to_owned(),
                    rationale: rationale.map(str::to_owned),
                    tags: tags.iter().map(|tag| (*tag).to_owned()).collect(),
                },
            );
        };

        add(
            FunctionFamily::Scalar,
            "abs()",
            "abs_numeric_edges",
            VerificationStatus::Passing,
            &["SELECT abs(-42)", "SELECT abs(-3.14)", "SELECT abs(0)"],
            &[
                (
                    "crates/fsqlite-harness/tests/bd_2wt_4_function_compat.rs",
                    "test_scalar_numeric_functions",
                    VerificationOracle::RusqliteDifferential,
                ),
                (
                    "crates/fsqlite-harness/tests/bd_2yqp6_5_1_function_parity_matrix.rs",
                    "test_numeric_edge_cases",
                    VerificationOracle::RusqliteDifferential,
                ),
            ],
            "Covers integer, float, zero, NULL, and large-value absolute value behaviour.",
            None,
            &["scalar", "math"],
        );
        add(
            FunctionFamily::Scalar,
            "length() / octet_length()",
            "string_and_octet_lengths",
            VerificationStatus::Passing,
            &[
                "SELECT length('hello')",
                "SELECT length(NULL)",
                "SELECT octet_length('hello')",
            ],
            &[
                (
                    "crates/fsqlite-harness/tests/bd_2wt_4_function_compat.rs",
                    "test_scalar_string_functions",
                    VerificationOracle::RusqliteDifferential,
                ),
                (
                    "crates/fsqlite-harness/tests/bd_2yqp6_5_1_function_parity_matrix.rs",
                    "test_string_edge_cases",
                    VerificationOracle::RusqliteDifferential,
                ),
            ],
            "Tracks text, empty string, NULL, integer coercion, and octet-length behaviour.",
            None,
            &["scalar", "string"],
        );
        add(
            FunctionFamily::Scalar,
            "typeof()",
            "runtime_type_introspection",
            VerificationStatus::Passing,
            &[
                "SELECT typeof(42)",
                "SELECT typeof(3.14)",
                "SELECT typeof(sqlite_version())",
            ],
            &[
                (
                    "crates/fsqlite-harness/tests/bd_2wt_4_function_compat.rs",
                    "test_scalar_string_functions",
                    VerificationOracle::RusqliteDifferential,
                ),
                (
                    "crates/fsqlite-harness/tests/bd_2yqp6_5_1_function_parity_matrix.rs",
                    "test_sqlite_meta_functions",
                    VerificationOracle::MetadataAssertion,
                ),
            ],
            "Validates type names for scalar literals and metadata-returning built-ins.",
            None,
            &["scalar", "type"],
        );
        add(
            FunctionFamily::Scalar,
            "upper() / lower()",
            "ascii_case_conversion",
            VerificationStatus::Passing,
            &["SELECT lower('HELLO')", "SELECT upper('hello')"],
            &[(
                "crates/fsqlite-harness/tests/bd_2wt_4_function_compat.rs",
                "test_scalar_string_functions",
                VerificationOracle::RusqliteDifferential,
            )],
            "Exercises mixed-case and NULL propagation semantics.",
            None,
            &["scalar", "string"],
        );
        add(
            FunctionFamily::Scalar,
            "hex() / unhex()",
            "hex_roundtrip_and_invalid_decode",
            VerificationStatus::Passing,
            &[
                "SELECT hex('ABC')",
                "SELECT unhex('414243')",
                "SELECT unhex('GG')",
            ],
            &[
                (
                    "crates/fsqlite-harness/tests/bd_2wt_4_function_compat.rs",
                    "test_scalar_string_functions",
                    VerificationOracle::RusqliteDifferential,
                ),
                (
                    "crates/fsqlite-harness/tests/bd_2yqp6_5_1_function_parity_matrix.rs",
                    "test_hex_unhex_parity",
                    VerificationOracle::RusqliteDifferential,
                ),
            ],
            "Includes encode/decode success, invalid hex, and blob-oriented edge cases.",
            None,
            &["scalar", "encoding"],
        );
        add(
            FunctionFamily::Scalar,
            "quote()",
            "literal_quoting_of_scalar_values",
            VerificationStatus::Passing,
            &[
                "SELECT quote('hello')",
                "SELECT quote(42)",
                "SELECT quote(NULL)",
            ],
            &[(
                "crates/fsqlite-harness/tests/bd_2wt_4_function_compat.rs",
                "test_scalar_string_functions",
                VerificationOracle::RusqliteDifferential,
            )],
            "Checks quoting of text, numeric, floating-point, and NULL values.",
            None,
            &["scalar", "literal"],
        );
        add(
            FunctionFamily::Scalar,
            "nullif() / ifnull() / coalesce()",
            "null_selection_and_short_circuiting",
            VerificationStatus::Passing,
            &[
                "SELECT coalesce(NULL, NULL, 42)",
                "SELECT ifnull(NULL, 2)",
                "SELECT nullif(1, 1)",
            ],
            &[
                (
                    "crates/fsqlite-harness/tests/bd_2wt_4_function_compat.rs",
                    "test_scalar_numeric_functions",
                    VerificationOracle::RusqliteDifferential,
                ),
                (
                    "crates/fsqlite-harness/tests/bd_2yqp6_5_1_function_parity_matrix.rs",
                    "test_null_propagation_extended",
                    VerificationOracle::RusqliteDifferential,
                ),
            ],
            "Captures NULL-elision behaviour and edge cases around all-NULL argument lists.",
            None,
            &["scalar", "null"],
        );
        add(
            FunctionFamily::Scalar,
            "printf() / format()",
            "formatted_string_output",
            VerificationStatus::Passing,
            &[
                "SELECT printf('%04d', 12)",
                "SELECT format('%s-%d', 'x', 7)",
            ],
            &[(
                "crates/fsqlite-harness/tests/bd_2yqp6_5_1_function_parity_matrix.rs",
                "test_format_printf_functions",
                VerificationOracle::RusqliteDifferential,
            )],
            "Standard formatting coverage for integer, string, width, precision, and mixed-specifier cases.",
            None,
            &["scalar", "string", "format"],
        );
        add(
            FunctionFamily::Scalar,
            "printf() / format()",
            "binding_sensitive_printf_variants",
            VerificationStatus::KnownBindingDifference,
            &[
                "SELECT printf('%c', 65)",
                "SELECT printf('%s', NULL)",
                "SELECT printf('%q', NULL)",
            ],
            &[(
                "crates/fsqlite-harness/tests/bd_2yqp6_5_1_function_parity_matrix.rs",
                "test_format_printf_functions",
                VerificationOracle::RusqliteDifferential,
            )],
            "Rows documented in the bead suite as rusqlite binding artefacts rather than engine parity failures.",
            Some(
                "rusqlite parameter/value conversions differ from C SQLite for selected printf specifiers and NULL formatting paths",
            ),
            &["scalar", "string", "format", "binding-diff"],
        );
        add(
            FunctionFamily::Scalar,
            "instr()",
            "substring_search",
            VerificationStatus::Passing,
            &[
                "SELECT instr('hello world', 'world')",
                "SELECT instr('hello', '')",
            ],
            &[(
                "crates/fsqlite-harness/tests/bd_2wt_4_function_compat.rs",
                "test_scalar_string_functions",
                VerificationOracle::RusqliteDifferential,
            )],
            "Covers found, missing, empty-needle, and NULL-input behaviour.",
            None,
            &["scalar", "string"],
        );
        add(
            FunctionFamily::Scalar,
            "trim() / ltrim() / rtrim()",
            "whitespace_and_char_trim",
            VerificationStatus::Passing,
            &[
                "SELECT trim('  hello  ')",
                "SELECT ltrim('  hello  ')",
                "SELECT rtrim('  hello  ')",
            ],
            &[(
                "crates/fsqlite-harness/tests/bd_2wt_4_function_compat.rs",
                "test_scalar_string_functions",
                VerificationOracle::RusqliteDifferential,
            )],
            "Exercises whitespace removal and custom-trim-character handling.",
            None,
            &["scalar", "string"],
        );
        add(
            FunctionFamily::Scalar,
            "replace()",
            "substring_replacement",
            VerificationStatus::Passing,
            &[
                "SELECT replace('hello world', 'world', 'earth')",
                "SELECT replace(NULL, 'a', 'b')",
            ],
            &[
                (
                    "crates/fsqlite-harness/tests/bd_2wt_4_function_compat.rs",
                    "test_scalar_string_functions",
                    VerificationOracle::RusqliteDifferential,
                ),
                (
                    "crates/fsqlite-harness/tests/bd_2yqp6_5_1_function_parity_matrix.rs",
                    "test_string_edge_cases",
                    VerificationOracle::RusqliteDifferential,
                ),
            ],
            "Includes ordinary replacement, no-match behaviour, and NULL propagation.",
            None,
            &["scalar", "string"],
        );
        add(
            FunctionFamily::Scalar,
            "substr() / substring()",
            "positive_negative_and_zero_length_slices",
            VerificationStatus::Passing,
            &[
                "SELECT substr('hello', 2, 3)",
                "SELECT substr('hello', -3)",
                "SELECT substr('hello', 2, 0)",
            ],
            &[
                (
                    "crates/fsqlite-harness/tests/bd_2wt_4_function_compat.rs",
                    "test_scalar_string_functions",
                    VerificationOracle::RusqliteDifferential,
                ),
                (
                    "crates/fsqlite-harness/tests/bd_2yqp6_5_1_function_parity_matrix.rs",
                    "test_string_edge_cases",
                    VerificationOracle::RusqliteDifferential,
                ),
            ],
            "Captures positive/negative offsets, zero lengths, and out-of-range slicing.",
            None,
            &["scalar", "string"],
        );
        add(
            FunctionFamily::Scalar,
            "unicode() / char()",
            "codepoint_conversion",
            VerificationStatus::Passing,
            &["SELECT char(72, 101, 108)", "SELECT unicode('A')"],
            &[(
                "crates/fsqlite-harness/tests/bd_2wt_4_function_compat.rs",
                "test_scalar_string_functions",
                VerificationOracle::RusqliteDifferential,
            )],
            "Checks basic codepoint-to-string and string-to-codepoint conversion paths.",
            None,
            &["scalar", "string", "unicode"],
        );
        add(
            FunctionFamily::Scalar,
            "unicode() / char()",
            "null_char_binding_variant",
            VerificationStatus::KnownBindingDifference,
            &["SELECT char(NULL)"],
            &[(
                "crates/fsqlite-harness/tests/bd_2yqp6_5_1_function_parity_matrix.rs",
                "test_string_edge_cases",
                VerificationOracle::RusqliteDifferential,
            )],
            "The bead suite records a rusqlite binding-level discrepancy for `char(NULL)` around embedded NUL handling.",
            Some(
                "binding-layer encoding differences make direct rusqlite comparison unreliable for NUL-producing `char(NULL)`",
            ),
            &["scalar", "string", "unicode", "binding-diff"],
        );
        add(
            FunctionFamily::Scalar,
            "zeroblob() / randomblob()",
            "blob_length_contracts",
            VerificationStatus::Passing,
            &["SELECT length(zeroblob(4))", "SELECT length(randomblob(4))"],
            &[
                (
                    "crates/fsqlite-harness/tests/bd_2wt_4_function_compat.rs",
                    "test_scalar_numeric_functions",
                    VerificationOracle::RusqliteDifferential,
                ),
                (
                    "crates/fsqlite-harness/tests/bd_2yqp6_5_1_function_parity_matrix.rs",
                    "test_probability_hint_and_random_contracts",
                    VerificationOracle::RusqliteDifferential,
                ),
                (
                    "crates/fsqlite-func/src/builtins.rs",
                    "test_randomblob_length",
                    VerificationOracle::LibraryUnit,
                ),
            ],
            "Uses deterministic length contracts for blob generators while leaving payload randomness unconstrained.",
            None,
            &["scalar", "blob", "random"],
        );
        add(
            FunctionFamily::Scalar,
            "random()",
            "integer_type_contract",
            VerificationStatus::Passing,
            &["SELECT typeof(random())"],
            &[
                (
                    "crates/fsqlite-harness/tests/bd_2yqp6_5_1_function_parity_matrix.rs",
                    "test_probability_hint_and_random_contracts",
                    VerificationOracle::RusqliteDifferential,
                ),
                (
                    "crates/fsqlite-func/src/builtins.rs",
                    "test_random_range",
                    VerificationOracle::LibraryUnit,
                ),
            ],
            "Verifies the deterministic portion of the contract: the function returns an integer-valued result.",
            None,
            &["scalar", "random"],
        );
        add(
            FunctionFamily::Scalar,
            "round()",
            "rounding_and_precision",
            VerificationStatus::Passing,
            &[
                "SELECT round(3.14159)",
                "SELECT round(3.14159, 2)",
                "SELECT round(-3.5)",
            ],
            &[
                (
                    "crates/fsqlite-harness/tests/bd_2wt_4_function_compat.rs",
                    "test_scalar_numeric_functions",
                    VerificationOracle::RusqliteDifferential,
                ),
                (
                    "crates/fsqlite-harness/tests/bd_2yqp6_5_1_function_parity_matrix.rs",
                    "test_numeric_edge_cases",
                    VerificationOracle::RusqliteDifferential,
                ),
            ],
            "Covers default precision, explicit precision, sign handling, and large numbers.",
            None,
            &["scalar", "math"],
        );
        add(
            FunctionFamily::Scalar,
            "sign()",
            "signum_over_integer_and_float_inputs",
            VerificationStatus::Passing,
            &["SELECT sign(42)", "SELECT sign(-42)", "SELECT sign(-3.14)"],
            &[(
                "crates/fsqlite-harness/tests/bd_2wt_4_function_compat.rs",
                "test_scalar_numeric_functions",
                VerificationOracle::RusqliteDifferential,
            )],
            "Verifies negative, positive, zero, and float signum semantics.",
            None,
            &["scalar", "math"],
        );
        add(
            FunctionFamily::Scalar,
            "iif()",
            "boolean_branch_selection",
            VerificationStatus::Passing,
            &["SELECT iif(1, 'yes', 'no')", "SELECT iif(0, 'yes', 'no')"],
            &[(
                "crates/fsqlite-harness/tests/bd_2wt_4_function_compat.rs",
                "test_scalar_numeric_functions",
                VerificationOracle::RusqliteDifferential,
            )],
            "Covers truthy and falsey branch selection.",
            None,
            &["scalar", "conditional"],
        );
        add(
            FunctionFamily::Scalar,
            "concat() / concat_ws()",
            "concatenation_with_separator_and_nulls",
            VerificationStatus::Passing,
            &[
                "SELECT concat('hello', ' ', 'world')",
                "SELECT concat_ws(',', 'a', 'b', 'c')",
            ],
            &[(
                "crates/fsqlite-harness/tests/bd_2wt_4_function_compat.rs",
                "test_scalar_string_functions",
                VerificationOracle::RusqliteDifferential,
            )],
            "Includes ordinary concatenation and separator-aware concatenation.",
            None,
            &["scalar", "string"],
        );
        add(
            FunctionFamily::Scalar,
            "likely() / unlikely()",
            "planner_hint_passthrough",
            VerificationStatus::Passing,
            &[
                "SELECT likely(0)",
                "SELECT unlikely(1)",
                "SELECT likely(NULL)",
            ],
            &[(
                "crates/fsqlite-harness/tests/bd_2yqp6_5_1_function_parity_matrix.rs",
                "test_probability_hint_and_random_contracts",
                VerificationOracle::RusqliteDifferential,
            )],
            "Checks that planner hints preserve argument values without introducing semantic drift.",
            None,
            &["scalar", "planner"],
        );
        add(
            FunctionFamily::Meta,
            "sqlite_version()",
            "version_string_contract",
            VerificationStatus::Passing,
            &["SELECT sqlite_version()", "SELECT typeof(sqlite_version())"],
            &[(
                "crates/fsqlite-harness/tests/bd_2yqp6_5_1_function_parity_matrix.rs",
                "test_sqlite_meta_functions",
                VerificationOracle::MetadataAssertion,
            )],
            "Confirms the surface contract for version-string reporting without pinning FrankenSQLite to rusqlite's exact build string.",
            None,
            &["meta"],
        );
        add(
            FunctionFamily::Meta,
            "sqlite_compileoption_used() / sqlite_compileoption_get()",
            "compile_option_introspection_contract",
            VerificationStatus::Passing,
            &[
                "SELECT sqlite_compileoption_used('THREADSAFE')",
                "SELECT sqlite_compileoption_used('SQLITE_ENABLE_ICU')",
                "SELECT sqlite_compileoption_get(0)",
                "PRAGMA compile_options",
            ],
            &[(
                "crates/fsqlite-harness/tests/bd_2yqp6_5_1_function_parity_matrix.rs",
                "test_sqlite_meta_functions",
                VerificationOracle::MetadataAssertion,
            )],
            "Verifies the exposed compile-option surface, including optional SQLITE_-prefix handling and alignment with PRAGMA compile_options.",
            None,
            &["meta", "introspection"],
        );
        add(
            FunctionFamily::Meta,
            "changes() / total_changes()",
            "stateful_change_counters",
            VerificationStatus::Passing,
            &["SELECT changes()", "SELECT total_changes()"],
            &[(
                "crates/fsqlite-harness/tests/bd_2yqp6_5_1_function_parity_matrix.rs",
                "test_stateful_meta_function_parity",
                VerificationOracle::RusqliteDifferential,
            )],
            "Tracks post-DML change counters after identical setup and insert sequences.",
            None,
            &["meta", "stateful"],
        );
        add(
            FunctionFamily::Meta,
            "last_insert_rowid()",
            "stateful_last_insert_rowid",
            VerificationStatus::Passing,
            &["SELECT last_insert_rowid()"],
            &[(
                "crates/fsqlite-harness/tests/bd_2yqp6_5_1_function_parity_matrix.rs",
                "test_stateful_meta_function_parity",
                VerificationOracle::RusqliteDifferential,
            )],
            "Compares rowid tracking after synchronized inserts into both engines.",
            None,
            &["meta", "stateful"],
        );
        add(
            FunctionFamily::Scalar,
            "glob() / like()",
            "pattern_matching",
            VerificationStatus::Passing,
            &[
                "SELECT like('a%', 'abc')",
                "SELECT glob('a*', 'abc')",
                "SELECT 'abc' LIKE 'a%'",
            ],
            &[(
                "crates/fsqlite-harness/tests/bd_2yqp6_5_1_function_parity_matrix.rs",
                "test_like_glob_parity",
                VerificationOracle::RusqliteDifferential,
            )],
            "Covers function-form and operator-form pattern matching cases.",
            None,
            &["scalar", "pattern"],
        );
        add(
            FunctionFamily::Scalar,
            "soundex()",
            "phonetic_encoding",
            VerificationStatus::Passing,
            &["SELECT soundex('Robert')", "SELECT soundex('Smith')"],
            &[(
                "crates/fsqlite-harness/tests/bd_2wt_4_function_compat.rs",
                "test_scalar_string_functions",
                VerificationOracle::RusqliteDifferential,
            )],
            "Verifies common English-name soundex encodings against SQLite.",
            None,
            &["scalar", "string"],
        );
        add(
            FunctionFamily::Scalar,
            "min() / max() scalar",
            "multi_argument_scalar_extrema",
            VerificationStatus::Passing,
            &["SELECT max(1, 5, 3, 9, 2)", "SELECT min(1, 5, 3, 9, 2)"],
            &[(
                "crates/fsqlite-harness/tests/bd_2wt_4_function_compat.rs",
                "test_scalar_numeric_functions",
                VerificationOracle::RusqliteDifferential,
            )],
            "Exercises multi-argument scalar extrema rather than aggregate semantics.",
            None,
            &["scalar", "extrema"],
        );
        add(
            FunctionFamily::Scalar,
            "load_extension()",
            "dynamic_extension_loading",
            VerificationStatus::Unsupported,
            &["SELECT load_extension('mod')"],
            &[],
            "Dynamic extension loading is intentionally not surfaced in the pure-Rust clean-room engine at this stage.",
            Some(
                "dynamic extension loading is intentionally not implemented; the taxonomy already marks this feature missing and the matrix documents it explicitly",
            ),
            &["scalar", "extension", "unsupported"],
        );
        add(
            FunctionFamily::Aggregate,
            "count()",
            "count_star_and_count_expr",
            VerificationStatus::Passing,
            &["SELECT count(*) FROM t", "SELECT count(val) FROM t"],
            &[
                (
                    "crates/fsqlite-harness/tests/bd_2wt_4_function_compat.rs",
                    "test_aggregate_functions",
                    VerificationOracle::RusqliteDifferential,
                ),
                (
                    "crates/fsqlite-core/src/connection.rs",
                    "test_conformance_window_functions",
                    VerificationOracle::RusqliteDifferential,
                ),
            ],
            "Includes `count(*)`, filtered counts, NULL-skipping counts, and window-count coverage.",
            None,
            &["aggregate"],
        );
        add(
            FunctionFamily::Aggregate,
            "sum() / total()",
            "sum_total_aggregate_and_window",
            VerificationStatus::Passing,
            &[
                "SELECT sum(val) FROM t",
                "SELECT total(val) FROM t",
                "SELECT sum(salary) OVER (ORDER BY id) FROM emp",
            ],
            &[
                (
                    "crates/fsqlite-harness/tests/bd_2wt_4_function_compat.rs",
                    "test_aggregate_functions",
                    VerificationOracle::RusqliteDifferential,
                ),
                (
                    "crates/fsqlite-core/src/connection.rs",
                    "test_conformance_window_functions",
                    VerificationOracle::RusqliteDifferential,
                ),
            ],
            "Captures aggregate and running-window accumulation semantics, including NULL behaviour for `total()` vs `sum()`.",
            None,
            &["aggregate", "window"],
        );
        add(
            FunctionFamily::Aggregate,
            "avg()",
            "average_over_integer_and_real_inputs",
            VerificationStatus::Passing,
            &[
                "SELECT avg(val) FROM t WHERE grp = 'a'",
                "SELECT avg(fval) FROM t WHERE grp = 'a'",
            ],
            &[
                (
                    "crates/fsqlite-harness/tests/bd_2wt_4_function_compat.rs",
                    "test_aggregate_functions",
                    VerificationOracle::RusqliteDifferential,
                ),
                (
                    "crates/fsqlite-core/src/connection.rs",
                    "test_conformance_window_functions",
                    VerificationOracle::RusqliteDifferential,
                ),
            ],
            "Checks integer and floating-point averaging, plus partitioned window averages.",
            None,
            &["aggregate", "window"],
        );
        add(
            FunctionFamily::Aggregate,
            "min() / max() aggregate",
            "aggregate_extrema",
            VerificationStatus::Passing,
            &["SELECT max(val) FROM t", "SELECT min(val) FROM t"],
            &[(
                "crates/fsqlite-harness/tests/bd_2wt_4_function_compat.rs",
                "test_aggregate_functions",
                VerificationOracle::RusqliteDifferential,
            )],
            "Covers grouped extrema and all-NULL groups.",
            None,
            &["aggregate", "extrema"],
        );
        add(
            FunctionFamily::Aggregate,
            "group_concat()",
            "string_aggregation_with_separator",
            VerificationStatus::Passing,
            &[
                "SELECT group_concat(grp) FROM t WHERE val IS NOT NULL",
                "SELECT group_concat(name, '|') FROM items",
            ],
            &[
                (
                    "crates/fsqlite-harness/tests/bd_2wt_4_function_compat.rs",
                    "test_aggregate_functions",
                    VerificationOracle::RusqliteDifferential,
                ),
                (
                    "crates/fsqlite-harness/tests/bd_2yqp6_5_1_function_parity_matrix.rs",
                    "test_aggregate_extended",
                    VerificationOracle::RusqliteDifferential,
                ),
            ],
            "Includes default separator, explicit separator, and NULL-input edge cases.",
            None,
            &["aggregate", "string"],
        );
        add(
            FunctionFamily::Window,
            "row_number()",
            "ordered_and_partitioned_row_number",
            VerificationStatus::Passing,
            &[
                "SELECT name, row_number() OVER (ORDER BY salary DESC) FROM emp",
                "SELECT name, row_number() OVER (PARTITION BY dept ORDER BY salary DESC) FROM emp",
            ],
            &[
                (
                    "crates/fsqlite-harness/tests/bd_2yqp6_5_1_function_parity_matrix.rs",
                    "test_window_function_parity",
                    VerificationOracle::RusqliteDifferential,
                ),
                (
                    "crates/fsqlite-core/src/connection.rs",
                    "test_conformance_window_functions",
                    VerificationOracle::RusqliteDifferential,
                ),
            ],
            "Covers global ordering and partition-aware numbering.",
            None,
            &["window", "ranking"],
        );
        add(
            FunctionFamily::Window,
            "rank() / dense_rank()",
            "rank_and_dense_rank",
            VerificationStatus::Passing,
            &[
                "SELECT name, rank() OVER (ORDER BY salary DESC) FROM emp",
                "SELECT name, dense_rank() OVER (ORDER BY salary DESC) FROM emp",
            ],
            &[
                (
                    "crates/fsqlite-harness/tests/bd_2yqp6_5_1_function_parity_matrix.rs",
                    "test_window_function_parity",
                    VerificationOracle::RusqliteDifferential,
                ),
                (
                    "crates/fsqlite-core/src/connection.rs",
                    "test_conformance_window_functions",
                    VerificationOracle::RusqliteDifferential,
                ),
            ],
            "Tracks gap-preserving and gap-free ranking semantics.",
            None,
            &["window", "ranking"],
        );
        add(
            FunctionFamily::Window,
            "ntile()",
            "bucket_partitioning",
            VerificationStatus::Passing,
            &["SELECT name, ntile(3) OVER (ORDER BY salary DESC) FROM emp"],
            &[(
                "crates/fsqlite-harness/tests/bd_2yqp6_5_1_function_parity_matrix.rs",
                "test_window_function_parity",
                VerificationOracle::RusqliteDifferential,
            )],
            "Covers bucket assignment across uneven partitions.",
            None,
            &["window", "ranking"],
        );
        add(
            FunctionFamily::Window,
            "lag() / lead()",
            "relative_row_access",
            VerificationStatus::Passing,
            &[
                "SELECT name, lag(salary, 1, -1) OVER (ORDER BY salary DESC) FROM emp",
                "SELECT name, lead(salary, 1, -1) OVER (ORDER BY salary DESC) FROM emp",
            ],
            &[(
                "crates/fsqlite-harness/tests/bd_2yqp6_5_1_function_parity_matrix.rs",
                "test_window_function_parity",
                VerificationOracle::RusqliteDifferential,
            )],
            "Covers default values and offset-based lookback/lookahead semantics.",
            None,
            &["window", "relative"],
        );
        add(
            FunctionFamily::Window,
            "first_value() / last_value() / nth_value()",
            "first_value_running_frame",
            VerificationStatus::Failing,
            &[
                "SELECT name, first_value(name) OVER (ORDER BY salary DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM emp",
            ],
            &[
                (
                    "crates/fsqlite-harness/tests/bd_2yqp6_5_1_function_parity_matrix.rs",
                    "test_window_function_parity",
                    VerificationOracle::RusqliteDifferential,
                ),
                (
                    "crates/fsqlite-func/src/window_builtins.rs",
                    "test_first_value_basic",
                    VerificationOracle::LibraryUnit,
                ),
            ],
            "Direct window-builtins unit coverage exists, but the differential suite still diverges from SQLite for running-frame first_value semantics.",
            Some(
                "Current engine/window-frame integration returns NULL after the first row instead of retaining the opening frame value across the running frame.",
            ),
            &["window", "value-access", "gap"],
        );
        add(
            FunctionFamily::Window,
            "first_value() / last_value() / nth_value()",
            "last_value_full_frame",
            VerificationStatus::Passing,
            &[
                "SELECT name, last_value(name) OVER (ORDER BY salary DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM emp",
            ],
            &[
                (
                    "crates/fsqlite-harness/tests/bd_2yqp6_5_1_function_parity_matrix.rs",
                    "test_window_function_parity",
                    VerificationOracle::RusqliteDifferential,
                ),
                (
                    "crates/fsqlite-func/src/window_builtins.rs",
                    "test_last_value_unbounded_following",
                    VerificationOracle::LibraryUnit,
                ),
            ],
            "Full-frame last_value semantics match SQLite in both the differential harness and direct window-builtins coverage.",
            None,
            &["window", "value-access"],
        );
        add(
            FunctionFamily::Window,
            "first_value() / last_value() / nth_value()",
            "nth_value_full_frame_second_row",
            VerificationStatus::Failing,
            &[
                "SELECT name, nth_value(name, 2) OVER (ORDER BY salary DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM emp",
            ],
            &[
                (
                    "crates/fsqlite-harness/tests/bd_2yqp6_5_1_function_parity_matrix.rs",
                    "test_window_function_parity",
                    VerificationOracle::RusqliteDifferential,
                ),
                (
                    "crates/fsqlite-func/src/window_builtins.rs",
                    "test_nth_value_basic",
                    VerificationOracle::LibraryUnit,
                ),
            ],
            "Low-level nth_value unit coverage exists, but full-frame SQL evaluation still diverges from SQLite in the parity harness.",
            Some(
                "Current engine/window-frame integration advances nth_value with the output cursor instead of holding the second frame row constant across the full partition.",
            ),
            &["window", "value-access", "gap"],
        );
        add(
            FunctionFamily::Window,
            "cume_dist() / percent_rank()",
            "cume_dist_peer_ties",
            VerificationStatus::Failing,
            &["SELECT name, cume_dist() OVER (ORDER BY salary DESC) FROM emp"],
            &[
                (
                    "crates/fsqlite-harness/tests/bd_2yqp6_5_1_function_parity_matrix.rs",
                    "test_window_function_parity",
                    VerificationOracle::RusqliteDifferential,
                ),
                (
                    "crates/fsqlite-func/src/window_builtins.rs",
                    "test_cume_dist_distinct",
                    VerificationOracle::LibraryUnit,
                ),
            ],
            "Distinct-value unit coverage exists, but the differential harness still exposes a peer-group handling gap for tied ORDER BY values.",
            Some(
                "Current cume_dist integration increments per row instead of assigning the peer group's cumulative distribution to tied rows.",
            ),
            &["window", "distribution", "gap"],
        );
        add(
            FunctionFamily::Window,
            "cume_dist() / percent_rank()",
            "percent_rank_global",
            VerificationStatus::Passing,
            &["SELECT name, percent_rank() OVER (ORDER BY salary DESC) FROM emp"],
            &[
                (
                    "crates/fsqlite-harness/tests/bd_2yqp6_5_1_function_parity_matrix.rs",
                    "test_window_function_parity",
                    VerificationOracle::RusqliteDifferential,
                ),
                (
                    "crates/fsqlite-func/src/window_builtins.rs",
                    "test_percent_rank_formula",
                    VerificationOracle::LibraryUnit,
                ),
            ],
            "percent_rank semantics currently match SQLite in the differential harness and direct formula checks.",
            None,
            &["window", "distribution"],
        );
        add(
            FunctionFamily::Datetime,
            "date() / time() / datetime()",
            "formatting_and_modifier_semantics",
            VerificationStatus::Passing,
            &[
                "SELECT date('2024-03-15', 'start of month')",
                "SELECT time('10:00:00', '+90 minutes')",
                "SELECT datetime('2024-03-15 22:00:00', '+3 hours')",
            ],
            &[
                (
                    "crates/fsqlite-harness/tests/bd_2yqp6_5_1_function_parity_matrix.rs",
                    "test_datetime_functions",
                    VerificationOracle::RusqliteDifferential,
                ),
                (
                    "crates/fsqlite-core/src/connection.rs",
                    "test_conformance_datetime_functions",
                    VerificationOracle::RusqliteDifferential,
                ),
            ],
            "Captures plain formatting plus modifier-driven adjustments.",
            None,
            &["datetime"],
        );
        add(
            FunctionFamily::Datetime,
            "julianday()",
            "julian_day_conversion",
            VerificationStatus::Passing,
            &[
                "SELECT julianday('2024-03-15')",
                "SELECT julianday('2024-03-15 12:00:00')",
            ],
            &[
                (
                    "crates/fsqlite-harness/tests/bd_2yqp6_5_1_function_parity_matrix.rs",
                    "test_datetime_functions",
                    VerificationOracle::RusqliteDifferential,
                ),
                (
                    "crates/fsqlite-func/src/datetime.rs",
                    "test_julianday_basic",
                    VerificationOracle::LibraryUnit,
                ),
            ],
            "Uses both differential and direct unit coverage for Julian day conversion.",
            None,
            &["datetime"],
        );
        add(
            FunctionFamily::Datetime,
            "strftime()",
            "strftime_specifier_surface",
            VerificationStatus::Passing,
            &[
                "SELECT strftime('%Y-%m-%d', '2024-03-15 14:30:00')",
                "SELECT strftime('%H:%M:%S', '2024-03-15 14:30:45')",
            ],
            &[
                (
                    "crates/fsqlite-harness/tests/bd_2yqp6_5_1_function_parity_matrix.rs",
                    "test_datetime_functions",
                    VerificationOracle::RusqliteDifferential,
                ),
                (
                    "crates/fsqlite-func/src/datetime.rs",
                    "test_strftime_all_specifiers_presence",
                    VerificationOracle::LibraryUnit,
                ),
            ],
            "Covers formatting directives, date/time fields, and NULL behaviour.",
            None,
            &["datetime", "format"],
        );
        add(
            FunctionFamily::Datetime,
            "unixepoch()",
            "unix_timestamp_conversion",
            VerificationStatus::Passing,
            &[
                "SELECT unixepoch('1970-01-01 00:00:00')",
                "SELECT unixepoch('2024-03-15 14:30:00')",
            ],
            &[
                (
                    "crates/fsqlite-harness/tests/bd_2yqp6_5_1_function_parity_matrix.rs",
                    "test_datetime_functions",
                    VerificationOracle::RusqliteDifferential,
                ),
                (
                    "crates/fsqlite-func/src/datetime.rs",
                    "test_unixepoch_known_date",
                    VerificationOracle::LibraryUnit,
                ),
            ],
            "Covers epoch conversion for zero, known dates, and modifier-driven numeric inputs.",
            None,
            &["datetime"],
        );
        add(
            FunctionFamily::Datetime,
            "timediff()",
            "interval_between_timestamps",
            VerificationStatus::Passing,
            &[
                "SELECT timediff('2024-03-15 14:30:00', '2024-03-15 13:00:00')",
                "SELECT timediff('2025-01-01 00:00:00', '2024-12-31 23:59:59')",
            ],
            &[(
                "crates/fsqlite-func/src/datetime.rs",
                "test_timediff_year_boundary",
                VerificationOracle::LibraryUnit,
            )],
            "Current evidence lives in the datetime built-ins unit suite; the matrix makes that explicit until a differential harness row is added.",
            None,
            &["datetime", "interval"],
        );
        add(
            FunctionFamily::Math,
            "Math: ceil/floor/trunc",
            "rounding_direction_functions",
            VerificationStatus::Passing,
            &[
                "SELECT ceil(3.2)",
                "SELECT floor(-3.7)",
                "SELECT trunc(-3.7)",
            ],
            &[
                (
                    "crates/fsqlite-harness/tests/bd_2wt_4_function_compat.rs",
                    "test_math_functions",
                    VerificationOracle::MathematicalOracle,
                ),
                (
                    "crates/fsqlite-core/src/connection.rs",
                    "test_math_ceil_floor",
                    VerificationOracle::EngineUnit,
                ),
            ],
            "Validated against fixed mathematical expectations because bundled rusqlite math support is not assumed.",
            None,
            &["math"],
        );
        add(
            FunctionFamily::Math,
            "Math: log/log2/log10/ln",
            "logarithmic_functions",
            VerificationStatus::Passing,
            &["SELECT ln(1)", "SELECT log10(100)", "SELECT log2(8)"],
            &[(
                "crates/fsqlite-harness/tests/bd_2wt_4_function_compat.rs",
                "test_math_functions",
                VerificationOracle::MathematicalOracle,
            )],
            "Uses closed-form expected values for the logarithmic family.",
            None,
            &["math"],
        );
        add(
            FunctionFamily::Math,
            "Math: exp/pow/sqrt",
            "exponential_power_and_root_functions",
            VerificationStatus::Passing,
            &["SELECT exp(1)", "SELECT pow(2, 10)", "SELECT sqrt(4)"],
            &[(
                "crates/fsqlite-harness/tests/bd_2wt_4_function_compat.rs",
                "test_math_functions",
                VerificationOracle::MathematicalOracle,
            )],
            "Validated against standard mathematical identities and expected outputs.",
            None,
            &["math"],
        );
        add(
            FunctionFamily::Math,
            "Math: sin/cos/tan/asin/acos/atan/atan2",
            "trigonometric_functions",
            VerificationStatus::Passing,
            &["SELECT sin(0)", "SELECT acos(1)", "SELECT atan2(1, 1)"],
            &[(
                "crates/fsqlite-harness/tests/bd_2wt_4_function_compat.rs",
                "test_math_functions",
                VerificationOracle::MathematicalOracle,
            )],
            "Tracks direct and inverse trigonometric behaviour using known-angle identities.",
            None,
            &["math"],
        );
        add(
            FunctionFamily::Math,
            "Math: pi/radians/degrees",
            "angle_constants_and_conversions",
            VerificationStatus::Passing,
            &["SELECT pi()", "SELECT degrees(pi())", "SELECT radians(180)"],
            &[(
                "crates/fsqlite-harness/tests/bd_2wt_4_function_compat.rs",
                "test_math_functions",
                VerificationOracle::MathematicalOracle,
            )],
            "Covers the constant and bidirectional angle-conversion helpers.",
            None,
            &["math"],
        );
        add(
            FunctionFamily::Math,
            "Math: mod",
            "floating_point_modulo_function",
            VerificationStatus::Passing,
            &["SELECT mod(7, 3)"],
            &[(
                "crates/fsqlite-harness/tests/bd_2wt_4_function_compat.rs",
                "test_math_functions",
                VerificationOracle::MathematicalOracle,
            )],
            "Validates modulo behaviour independently of SQL `%` operator handling.",
            None,
            &["math"],
        );

        Self {
            schema_version: MATRIX_SCHEMA_VERSION,
            bead_id: BEAD_ID.to_owned(),
            target_sqlite_version: TARGET_SQLITE_VERSION.to_owned(),
            variants,
        }
    }

    /// Validate structural and coverage invariants.
    #[must_use]
    pub fn validate(&self) -> Vec<String> {
        let mut violations = Vec::new();
        let feature_catalog = builtin_feature_catalog();
        let mut covered = BTreeSet::new();

        for variant in self.variants.values() {
            covered.insert(variant.feature_id.clone());

            if variant.feature_title.is_empty() {
                violations.push(format!("{} has empty feature_title", variant.id));
            }
            if variant.variant_name.is_empty() {
                violations.push(format!("{} has empty variant_name", variant.id));
            }
            if let Some((expected_id, expected_status)) =
                feature_catalog.get(&variant.feature_title)
            {
                if expected_id != &variant.feature_id {
                    violations.push(format!(
                        "{} feature/title mismatch: expected {} for {}",
                        variant.id, expected_id, variant.feature_title
                    ));
                }
                if *expected_status != variant.declared_status {
                    violations.push(format!(
                        "{} declared status mismatch for {}",
                        variant.id, variant.feature_title
                    ));
                }
            } else {
                violations.push(format!(
                    "{} references unknown built-in feature title {}",
                    variant.id, variant.feature_title
                ));
            }

            match variant.verification_status {
                VerificationStatus::Passing => {
                    if variant.coverage_links.is_empty() {
                        violations.push(format!(
                            "{} is {:?} but has no coverage links",
                            variant.id, variant.verification_status
                        ));
                    }
                    if variant.representative_sql.is_empty() {
                        violations.push(format!(
                            "{} is {:?} but has no representative SQL",
                            variant.id, variant.verification_status
                        ));
                    }
                }
                VerificationStatus::KnownBindingDifference | VerificationStatus::Failing => {
                    if variant.coverage_links.is_empty() {
                        violations.push(format!(
                            "{} is {:?} but has no coverage links",
                            variant.id, variant.verification_status
                        ));
                    }
                    if variant.representative_sql.is_empty() {
                        violations.push(format!(
                            "{} is {:?} but has no representative SQL",
                            variant.id, variant.verification_status
                        ));
                    }
                    if variant.rationale.is_none() {
                        violations.push(format!(
                            "{} is {:?} but lacks rationale",
                            variant.id, variant.verification_status
                        ));
                    }
                }
                VerificationStatus::Unsupported => {
                    if variant.rationale.is_none() {
                        violations
                            .push(format!("{} is unsupported but lacks rationale", variant.id));
                    }
                }
            }

            for link in &variant.coverage_links {
                if link.test_file.is_empty() || link.test_name.is_empty() {
                    violations.push(format!(
                        "{} contains incomplete coverage link {:?}",
                        variant.id, link
                    ));
                }
            }
        }

        for (title, (feature_id, _status)) in &feature_catalog {
            if !covered.contains(feature_id) {
                violations.push(format!(
                    "builtin feature {title} ({feature_id}) missing from parity matrix"
                ));
            }
        }

        violations
    }

    /// Return rows sorted deterministically by row ID.
    #[must_use]
    pub fn sorted_variants(&self) -> Vec<&BuiltinFunctionVariant> {
        self.variants.values().collect()
    }

    /// Return rows for a given function family.
    #[must_use]
    pub fn variants_by_family(&self, family: FunctionFamily) -> Vec<&BuiltinFunctionVariant> {
        self.variants
            .values()
            .filter(|variant| variant.family == family)
            .collect()
    }

    /// Compute a deterministic summary.
    #[must_use]
    pub fn summary(&self) -> MatrixSummary {
        let mut variants_by_family = BTreeMap::new();
        let mut variants_by_status = BTreeMap::new();
        let mut features = BTreeSet::new();

        for variant in self.variants.values() {
            features.insert(variant.feature_id.clone());
            *variants_by_family
                .entry(variant.family.as_str().to_owned())
                .or_insert(0) += 1;
            *variants_by_status
                .entry(variant.verification_status.as_str().to_owned())
                .or_insert(0) += 1;
        }

        MatrixSummary {
            total_variants: self.variants.len(),
            total_features: features.len(),
            variants_by_family,
            variants_by_status,
        }
    }

    /// Serialize to deterministic JSON.
    ///
    /// # Errors
    ///
    /// Returns an error when serialization fails.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }
}

fn builtin_feature_catalog() -> BTreeMap<String, (FeatureId, ParityStatus)> {
    let universe = build_canonical_universe();
    let mut features = BTreeMap::new();
    for feature in universe.features_by_category(FeatureCategory::BuiltinFunctions) {
        features.insert(feature.title.clone(), (feature.id.clone(), feature.status));
    }
    features
}

fn coverage_links(seed: &[(&str, &str, VerificationOracle)]) -> Vec<CoverageLink> {
    seed.iter()
        .map(|(test_file, test_name, oracle)| CoverageLink {
            test_file: (*test_file).to_owned(),
            test_name: (*test_name).to_owned(),
            oracle: *oracle,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_matrix_validates() {
        let matrix = BuiltinFunctionParityMatrix::canonical();
        let diagnostics = matrix.validate();
        assert!(diagnostics.is_empty(), "Validation failed: {diagnostics:?}");
    }

    #[test]
    fn canonical_matrix_covers_all_builtin_features() {
        let matrix = BuiltinFunctionParityMatrix::canonical();
        let summary = matrix.summary();
        assert_eq!(
            summary.total_features, 50,
            "Unexpected built-in feature count"
        );
        assert!(
            summary.total_variants >= 49,
            "Expected at least one row per feature"
        );
    }

    #[test]
    fn supported_variants_have_evidence_links() {
        let matrix = BuiltinFunctionParityMatrix::canonical();
        for variant in matrix.sorted_variants() {
            if variant.verification_status != VerificationStatus::Unsupported {
                assert!(
                    !variant.coverage_links.is_empty(),
                    "{} should have evidence links",
                    variant.id
                );
            }
        }
    }

    #[test]
    fn load_extension_is_explicitly_unsupported() {
        let matrix = BuiltinFunctionParityMatrix::canonical();
        let entries: Vec<_> = matrix
            .sorted_variants()
            .into_iter()
            .filter(|variant| variant.feature_title == "load_extension()")
            .collect();
        assert_eq!(entries.len(), 1);
        let entry = entries[0];
        assert_eq!(entry.verification_status, VerificationStatus::Unsupported);
        assert!(entry.rationale.is_some());
    }

    #[test]
    fn known_binding_differences_are_documented() {
        let matrix = BuiltinFunctionParityMatrix::canonical();
        let binding_diffs: Vec<_> = matrix
            .sorted_variants()
            .into_iter()
            .filter(|variant| {
                variant.verification_status == VerificationStatus::KnownBindingDifference
            })
            .collect();
        assert!(
            binding_diffs.len() >= 2,
            "Expected at least two documented binding differences"
        );
        for entry in &binding_diffs {
            assert!(
                entry.rationale.is_some(),
                "{} should carry rationale",
                entry.id
            );
        }
    }

    #[test]
    fn failing_variants_are_documented() {
        let matrix = BuiltinFunctionParityMatrix::canonical();
        let failing: Vec<_> = matrix
            .sorted_variants()
            .into_iter()
            .filter(|variant| variant.verification_status == VerificationStatus::Failing)
            .collect();
        assert!(
            failing.len() >= 3,
            "Expected documented failing differential variants"
        );
        for entry in &failing {
            assert!(
                entry.rationale.is_some(),
                "{} should carry rationale",
                entry.id
            );
        }

        let variant_names: BTreeSet<_> = failing
            .iter()
            .map(|variant| variant.variant_name.as_str())
            .collect();
        assert!(variant_names.contains("first_value_running_frame"));
        assert!(variant_names.contains("nth_value_full_frame_second_row"));
        assert!(variant_names.contains("cume_dist_peer_ties"));
    }

    #[test]
    fn matrix_spans_all_major_families() {
        let matrix = BuiltinFunctionParityMatrix::canonical();
        for family in [
            FunctionFamily::Scalar,
            FunctionFamily::Aggregate,
            FunctionFamily::Window,
            FunctionFamily::Datetime,
            FunctionFamily::Math,
            FunctionFamily::Meta,
        ] {
            assert!(
                !matrix.variants_by_family(family).is_empty(),
                "Missing family {}",
                family.as_str()
            );
        }
    }

    #[test]
    fn json_round_trip() {
        let matrix = BuiltinFunctionParityMatrix::canonical();
        let json = matrix.to_json().expect("serialize");
        let restored: BuiltinFunctionParityMatrix =
            serde_json::from_str(&json).expect("deserialize");
        assert_eq!(matrix.variants.len(), restored.variants.len());
        assert_eq!(matrix.bead_id, restored.bead_id);
        assert_eq!(matrix.target_sqlite_version, restored.target_sqlite_version);
    }
}
