//! Collation callback trait, built-in collations, and registry (§9.4, §13.6).
//!
//! Collations are pure comparators used by ORDER BY, GROUP BY, DISTINCT,
//! and index traversal. They are open extension points.
//!
//! `compare` is intentionally CPU-only and does not accept `&Cx`.
//!
//! The [`CollationRegistry`] maps case-insensitive names to collation
//! implementations and is pre-populated with the three built-in collations.
//!
//! # Contract
//!
//! Implementations **must** be:
//! - **Deterministic**: same inputs always produce the same output.
//! - **Antisymmetric**: `compare(a, b)` is the reverse of `compare(b, a)`.
//! - **Transitive**: if `a < b` and `b < c`, then `a < c`.
#![allow(clippy::unnecessary_literal_bound)]

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

use tracing::{debug, info};

/// A collation comparator.
///
/// Implementations define total ordering over UTF-8 byte strings.
///
/// Built-in collations: [`BinaryCollation`] (memcmp), [`NoCaseCollation`]
/// (ASCII case-insensitive), [`RtrimCollation`] (trailing-space-insensitive).
pub trait CollationFunction: Send + Sync {
    /// Collation name (for `COLLATE name`).
    fn name(&self) -> &str;

    /// Compare two UTF-8 byte slices.
    ///
    /// Must be deterministic, antisymmetric, and transitive.
    fn compare(&self, left: &[u8], right: &[u8]) -> Ordering;
}

// ── Built-in collations ──────────────────────────────────────────────────

/// BINARY collation: raw `memcmp` byte comparison.
///
/// This is SQLite's default collation. Comparison is byte-by-byte with no
/// locale or case folding.
pub struct BinaryCollation;

impl CollationFunction for BinaryCollation {
    fn name(&self) -> &str {
        "BINARY"
    }

    fn compare(&self, left: &[u8], right: &[u8]) -> Ordering {
        left.cmp(right)
    }
}

/// NOCASE collation: ASCII case-insensitive comparison.
///
/// Only folds ASCII letters (`a-z` → `A-Z`). Non-ASCII bytes are compared
/// as-is. For full Unicode case folding, use the ICU extension (§14.6).
pub struct NoCaseCollation;

impl CollationFunction for NoCaseCollation {
    fn name(&self) -> &str {
        "NOCASE"
    }

    fn compare(&self, left: &[u8], right: &[u8]) -> Ordering {
        let l = left.iter().map(u8::to_ascii_uppercase);
        let r = right.iter().map(u8::to_ascii_uppercase);
        l.cmp(r)
    }
}

/// RTRIM collation: trailing-space-insensitive comparison.
///
/// Trailing ASCII spaces (`0x20`) are stripped before comparison.
/// All other characters (including tabs, non-breaking spaces) are significant.
pub struct RtrimCollation;

impl CollationFunction for RtrimCollation {
    fn name(&self) -> &str {
        "RTRIM"
    }

    fn compare(&self, left: &[u8], right: &[u8]) -> Ordering {
        let l = strip_trailing_spaces(left);
        let r = strip_trailing_spaces(right);
        l.cmp(r)
    }
}

fn strip_trailing_spaces(s: &[u8]) -> &[u8] {
    let mut end = s.len();
    while end > 0 && s[end - 1] == b' ' {
        end -= 1;
    }
    &s[..end]
}

// ── Collation registry ─────────────────────────────────────────────────

/// Registry for collation functions, keyed by case-insensitive name.
///
/// Pre-populated with the three built-in collations: BINARY, NOCASE, RTRIM.
/// Custom collations can be registered via [`CollationRegistry::register`].
pub struct CollationRegistry {
    collations: HashMap<String, Arc<dyn CollationFunction>>,
}

impl Default for CollationRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl CollationRegistry {
    /// Create a new registry pre-populated with BINARY, NOCASE, and RTRIM.
    #[must_use]
    pub fn new() -> Self {
        let mut collations = HashMap::with_capacity(3);
        collations.insert(
            "BINARY".to_owned(),
            Arc::new(BinaryCollation) as Arc<dyn CollationFunction>,
        );
        collations.insert(
            "NOCASE".to_owned(),
            Arc::new(NoCaseCollation) as Arc<dyn CollationFunction>,
        );
        collations.insert(
            "RTRIM".to_owned(),
            Arc::new(RtrimCollation) as Arc<dyn CollationFunction>,
        );
        Self { collations }
    }

    /// Register a custom collation. Returns the previous collation with the
    /// same name if one existed (overwrites).
    ///
    /// Collation names are case-insensitive.
    pub fn register<C: CollationFunction + 'static>(
        &mut self,
        collation: C,
    ) -> Option<Arc<dyn CollationFunction>> {
        let name = collation.name().to_ascii_uppercase();
        info!(collation_name = %name, deterministic = true, "custom collation registration");
        self.collations.insert(name, Arc::new(collation))
    }

    /// Look up a collation by name (case-insensitive).
    ///
    /// Returns `None` if no collation with the given name is registered.
    #[must_use]
    pub fn find(&self, name: &str) -> Option<Arc<dyn CollationFunction>> {
        let canon = name.to_ascii_uppercase();
        let result = self.collations.get(&canon).cloned();
        debug!(
            collation = %canon,
            hit = result.is_some(),
            "collation registry lookup"
        );
        result
    }

    /// Check whether a collation with the given name is registered.
    #[must_use]
    pub fn contains(&self, name: &str) -> bool {
        self.collations.contains_key(&name.to_ascii_uppercase())
    }
}

// ── Collation selection ─────────────────────────────────────────────────

/// Source of a collation for precedence resolution (§13.6).
///
/// When two operands in a comparison have different collation sources,
/// the higher-precedence source wins.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CollationSource {
    /// Explicit `COLLATE` clause in the expression (highest precedence).
    Explicit,
    /// Column schema collation (`CREATE TABLE ... COLLATE NOCASE`).
    Schema,
    /// Default (BINARY) when no other source applies (lowest precedence).
    Default,
}

/// An operand's collation annotation: the collation name and where it came from.
#[derive(Debug, Clone)]
pub struct CollationAnnotation {
    /// Collation name (e.g. "BINARY", "NOCASE").
    pub name: String,
    /// Where this collation was specified.
    pub source: CollationSource,
}

/// Resolve which collation to use for a binary comparison (§13.6).
///
/// Precedence rules:
/// 1. Explicit `COLLATE` clause wins. If both operands have explicit
///    collations, the leftmost (LHS) wins.
/// 2. Schema collation from column definition.
/// 3. Default BINARY.
///
/// Returns the collation name to use for the comparison.
#[must_use]
pub fn resolve_collation(lhs: &CollationAnnotation, rhs: &CollationAnnotation) -> String {
    // Precedence: Explicit > Schema > Default. Ties go to LHS (leftmost).
    let result = match (lhs.source, rhs.source) {
        (_, CollationSource::Explicit) if lhs.source != CollationSource::Explicit => &rhs.name,
        (CollationSource::Default, CollationSource::Schema) => &rhs.name,
        _ => &lhs.name,
    };
    debug!(
        collation = %result,
        lhs_source = ?lhs.source,
        rhs_source = ?rhs.source,
        context = "COMPARE",
        "collation selection"
    );
    result.clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Built-in collation tests (bd-1dc9 + bd-ef4j) ───────────────────

    #[test]
    fn test_collation_binary_memcmp() {
        let coll = BinaryCollation;
        assert_eq!(coll.compare(b"abc", b"abc"), Ordering::Equal);
        assert_eq!(coll.compare(b"abc", b"abd"), Ordering::Less);
        assert_eq!(coll.compare(b"abd", b"abc"), Ordering::Greater);
        // Mixed case: uppercase < lowercase in byte ordering
        assert_eq!(coll.compare(b"ABC", b"abc"), Ordering::Less);
        // Non-ASCII UTF-8: multibyte sequences
        assert_eq!(
            coll.compare("café".as_bytes(), "café".as_bytes()),
            Ordering::Equal
        );
        assert_ne!(coll.compare("über".as_bytes(), b"uber"), Ordering::Equal);
    }

    #[test]
    fn test_collation_binary_basic() {
        let coll = BinaryCollation;
        // 'ABC' < 'abc' under BINARY (uppercase bytes 0x41-0x5A < 0x61-0x7A)
        assert_eq!(coll.compare(b"ABC", b"abc"), Ordering::Less);
        // Byte-by-byte, not character-aware
        assert_eq!(coll.compare(b"\x00", b"\x01"), Ordering::Less);
        assert_eq!(coll.compare(b"\xff", b"\x00"), Ordering::Greater);
    }

    #[test]
    fn test_collation_nocase_ascii() {
        let coll = NoCaseCollation;
        assert_eq!(coll.compare(b"ABC", b"abc"), Ordering::Equal);
        assert_eq!(coll.compare(b"Alice", b"alice"), Ordering::Equal);
        // `[` (0x5B) < `a` (0x61) normally, but NOCASE: `[` (0x5B) > `A` (0x41)
        assert_eq!(coll.compare(b"[", b"a"), Ordering::Greater);
    }

    #[test]
    fn test_collation_nocase_ascii_only() {
        let coll = NoCaseCollation;
        // Non-ASCII bytes are NOT folded — 'Ä' (0xC3 0x84) != 'ä' (0xC3 0xA4)
        assert_ne!(
            coll.compare("Ä".as_bytes(), "ä".as_bytes()),
            Ordering::Equal,
            "NOCASE must NOT fold non-ASCII"
        );
        // Only ASCII A-Z are folded
        assert_eq!(coll.compare(b"Z", b"z"), Ordering::Equal);
        assert_eq!(coll.compare(b"[", b"["), Ordering::Equal);
        // 0x5B '[' is just past 'Z' (0x5A) — must NOT be folded
        assert_ne!(coll.compare(b"[", b"{"), Ordering::Equal);
    }

    #[test]
    fn test_collation_rtrim() {
        let coll = RtrimCollation;
        // Trailing spaces are ignored
        assert_eq!(coll.compare(b"hello   ", b"hello"), Ordering::Equal);
        assert_eq!(coll.compare(b"hello", b"hello   "), Ordering::Equal);
        assert_eq!(coll.compare(b"hello   ", b"hello   "), Ordering::Equal);
        // Non-space trailing chars are NOT ignored
        assert_ne!(coll.compare(b"hello!", b"hello"), Ordering::Equal);
        // Trailing space + different content
        assert_ne!(coll.compare(b"hello ", b"hello!"), Ordering::Equal);
    }

    #[test]
    fn test_collation_rtrim_tabs_not_stripped() {
        let coll = RtrimCollation;
        // Only 0x20 spaces are stripped, NOT tabs (0x09)
        assert_ne!(
            coll.compare(b"hello\t", b"hello"),
            Ordering::Equal,
            "RTRIM must NOT strip tabs"
        );
        // Not non-breaking space either
        assert_ne!(
            coll.compare(b"hello\xc2\xa0", b"hello"),
            Ordering::Equal,
            "RTRIM must NOT strip non-breaking spaces"
        );
    }

    #[test]
    fn test_collation_properties_antisymmetric() {
        let collations: Vec<Box<dyn CollationFunction>> = vec![
            Box::new(BinaryCollation),
            Box::new(NoCaseCollation),
            Box::new(RtrimCollation),
        ];

        let pairs: &[(&[u8], &[u8])] = &[
            (b"abc", b"def"),
            (b"hello", b"world"),
            (b"ABC", b"abc"),
            (b"hello   ", b"hello"),
        ];

        for coll in &collations {
            for &(a, b) in pairs {
                let forward = coll.compare(a, b);
                let reverse = coll.compare(b, a);
                assert_eq!(
                    forward,
                    reverse.reverse(),
                    "{}: compare({:?}, {:?}) = {forward:?}, but reverse = {reverse:?}",
                    coll.name(),
                    std::str::from_utf8(a).unwrap_or("?"),
                    std::str::from_utf8(b).unwrap_or("?"),
                );
            }
        }
    }

    #[test]
    fn test_collation_properties_transitive() {
        let coll = BinaryCollation;
        let a = b"apple";
        let b = b"banana";
        let c = b"cherry";

        // a < b and b < c => a < c
        assert_eq!(coll.compare(a, b), Ordering::Less);
        assert_eq!(coll.compare(b, c), Ordering::Less);
        assert_eq!(coll.compare(a, c), Ordering::Less);
    }

    #[test]
    fn test_collation_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<BinaryCollation>();
        assert_send_sync::<NoCaseCollation>();
        assert_send_sync::<RtrimCollation>();
    }

    // ── Registry tests (bd-ef4j) ────────────────────────────────────────

    #[test]
    fn test_registry_preloaded_builtins() {
        let reg = CollationRegistry::new();
        assert!(reg.contains("BINARY"));
        assert!(reg.contains("NOCASE"));
        assert!(reg.contains("RTRIM"));

        let binary = reg.find("BINARY").expect("BINARY must be pre-registered");
        assert_eq!(binary.compare(b"a", b"b"), Ordering::Less);

        let nocase = reg.find("NOCASE").expect("NOCASE must be pre-registered");
        assert_eq!(nocase.compare(b"ABC", b"abc"), Ordering::Equal);

        let rtrim = reg.find("RTRIM").expect("RTRIM must be pre-registered");
        assert_eq!(rtrim.compare(b"x  ", b"x"), Ordering::Equal);
    }

    struct ReverseCollation;

    impl CollationFunction for ReverseCollation {
        fn name(&self) -> &str {
            "REVERSE"
        }

        fn compare(&self, left: &[u8], right: &[u8]) -> Ordering {
            right.cmp(left)
        }
    }

    #[test]
    fn test_registry_custom_collation_registration() {
        let mut reg = CollationRegistry::new();

        let prev = reg.register(ReverseCollation);
        assert!(prev.is_none(), "no prior REVERSE collation");
        assert!(reg.contains("REVERSE"));

        let coll = reg.find("reverse").expect("case-insensitive lookup");
        assert_eq!(coll.compare(b"a", b"z"), Ordering::Greater);
    }

    struct AlwaysEqualCollation;

    impl CollationFunction for AlwaysEqualCollation {
        fn name(&self) -> &str {
            "BINARY"
        }

        fn compare(&self, _left: &[u8], _right: &[u8]) -> Ordering {
            Ordering::Equal
        }
    }

    #[test]
    fn test_registry_overwrite_builtin() {
        let mut reg = CollationRegistry::new();

        let prev = reg.register(AlwaysEqualCollation);
        assert!(prev.is_some(), "should return previous BINARY collation");

        let coll = reg.find("BINARY").unwrap();
        assert_eq!(
            coll.compare(b"a", b"z"),
            Ordering::Equal,
            "custom overwrite must take effect"
        );
    }

    #[test]
    fn test_registry_unregistered_returns_none() {
        let reg = CollationRegistry::new();
        assert!(reg.find("NONEXISTENT").is_none());
        assert!(!reg.contains("NONEXISTENT"));
    }

    #[test]
    fn test_registry_name_case_insensitive() {
        let reg = CollationRegistry::new();
        // BINARY = binary = Binary
        assert!(reg.find("BINARY").is_some());
        assert!(reg.find("binary").is_some());
        assert!(reg.find("Binary").is_some());
        assert!(reg.find("bInArY").is_some());

        // Contains is also case-insensitive
        assert!(reg.contains("nocase"));
        assert!(reg.contains("NOCASE"));
        assert!(reg.contains("NoCase"));
    }

    // ── Collation selection / precedence tests (bd-ef4j) ────────────────

    fn ann(name: &str, source: CollationSource) -> CollationAnnotation {
        CollationAnnotation {
            name: name.to_owned(),
            source,
        }
    }

    #[test]
    fn test_collation_selection_explicit_wins() {
        // Explicit COLLATE NOCASE on LHS vs default BINARY on RHS
        let result = resolve_collation(
            &ann("NOCASE", CollationSource::Explicit),
            &ann("BINARY", CollationSource::Default),
        );
        assert_eq!(result, "NOCASE");
    }

    #[test]
    fn test_collation_selection_explicit_rhs_wins_over_default() {
        let result = resolve_collation(
            &ann("BINARY", CollationSource::Default),
            &ann("RTRIM", CollationSource::Explicit),
        );
        assert_eq!(result, "RTRIM");
    }

    #[test]
    fn test_collation_selection_leftmost_explicit_wins() {
        // When both operands have explicit COLLATE, leftmost (LHS) wins
        let result = resolve_collation(
            &ann("NOCASE", CollationSource::Explicit),
            &ann("RTRIM", CollationSource::Explicit),
        );
        assert_eq!(result, "NOCASE");
    }

    #[test]
    fn test_collation_selection_schema_over_default() {
        let result = resolve_collation(
            &ann("NOCASE", CollationSource::Schema),
            &ann("BINARY", CollationSource::Default),
        );
        assert_eq!(result, "NOCASE");
    }

    #[test]
    fn test_collation_selection_schema_rhs_over_default() {
        let result = resolve_collation(
            &ann("BINARY", CollationSource::Default),
            &ann("NOCASE", CollationSource::Schema),
        );
        assert_eq!(result, "NOCASE");
    }

    #[test]
    fn test_collation_selection_explicit_over_schema() {
        let result = resolve_collation(
            &ann("RTRIM", CollationSource::Explicit),
            &ann("NOCASE", CollationSource::Schema),
        );
        assert_eq!(result, "RTRIM");
    }

    #[test]
    fn test_collation_selection_default_binary() {
        let result = resolve_collation(
            &ann("BINARY", CollationSource::Default),
            &ann("BINARY", CollationSource::Default),
        );
        assert_eq!(result, "BINARY");
    }

    // ── min/max respect collation tests (bd-ef4j) ───────────────────────

    #[test]
    fn test_min_respects_collation() {
        // Under BINARY: 'ABC' < 'abc' (uppercase bytes < lowercase bytes)
        let binary = BinaryCollation;
        let binary_min = if binary.compare(b"ABC", b"abc") == Ordering::Less {
            "ABC"
        } else {
            "abc"
        };
        assert_eq!(binary_min, "ABC");

        // Under NOCASE: 'ABC' == 'abc', so min could be either (both equal)
        let nocase = NoCaseCollation;
        assert_eq!(nocase.compare(b"ABC", b"abc"), Ordering::Equal);
    }

    #[test]
    fn test_max_respects_collation() {
        let binary = BinaryCollation;
        // Under BINARY: 'abc' > 'ABC'
        let binary_max = if binary.compare(b"abc", b"ABC") == Ordering::Greater {
            "abc"
        } else {
            "ABC"
        };
        assert_eq!(binary_max, "abc");
    }

    #[test]
    fn test_collation_aware_sort() {
        // Simulate ORDER BY with NOCASE collation
        let nocase = NoCaseCollation;
        let mut data: Vec<&[u8]> = vec![b"Banana", b"apple", b"Cherry", b"date"];
        data.sort_by(|a, b| nocase.compare(a, b));

        // NOCASE sort: apple < banana < cherry < date
        assert_eq!(data[0], b"apple");
        assert_eq!(data[1], b"Banana");
        assert_eq!(data[2], b"Cherry");
        assert_eq!(data[3], b"date");
    }

    #[test]
    fn test_collation_aware_group_by() {
        // Under NOCASE, 'ABC' and 'abc' are the same group
        let nocase = NoCaseCollation;
        let items: Vec<&[u8]> = vec![b"ABC", b"abc", b"Abc", b"def", b"DEF"];
        let mut groups: Vec<Vec<&[u8]>> = Vec::new();

        // Simple grouping by sorting then collecting equal runs
        let mut sorted = items;
        sorted.sort_by(|a, b| nocase.compare(a, b));

        let mut current_group: Vec<&[u8]> = vec![sorted[0]];
        for window in sorted.windows(2) {
            if nocase.compare(window[0], window[1]) != Ordering::Equal {
                groups.push(std::mem::take(&mut current_group));
            }
            current_group.push(window[1]);
        }
        groups.push(current_group);

        // Two groups: {ABC, abc, Abc} and {def, DEF}
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].len(), 3);
        assert_eq!(groups[1].len(), 2);
    }

    #[test]
    fn test_collation_aware_distinct() {
        // Under NOCASE, SELECT DISTINCT should deduplicate 'ABC' and 'abc'
        let nocase = NoCaseCollation;
        let items: Vec<&[u8]> = vec![b"ABC", b"abc", b"Abc", b"def", b"DEF"];

        let mut distinct: Vec<&[u8]> = Vec::new();
        for item in &items {
            let already = distinct
                .iter()
                .any(|d| nocase.compare(d, item) == Ordering::Equal);
            if !already {
                distinct.push(item);
            }
        }

        // Should have 2 distinct values: one from {ABC/abc/Abc} and one from {def/DEF}
        assert_eq!(distinct.len(), 2);
    }

    #[test]
    fn test_registry_default_impl() {
        // Verify Default trait implementation
        let reg = CollationRegistry::default();
        assert!(reg.contains("BINARY"));
        assert!(reg.contains("NOCASE"));
        assert!(reg.contains("RTRIM"));
    }

    #[test]
    fn test_collation_annotation_debug() {
        let ann = CollationAnnotation {
            name: "NOCASE".to_owned(),
            source: CollationSource::Explicit,
        };
        let debug_str = format!("{ann:?}");
        assert!(debug_str.contains("NOCASE"));
        assert!(debug_str.contains("Explicit"));
    }

    #[test]
    fn test_collation_source_equality() {
        assert_eq!(CollationSource::Explicit, CollationSource::Explicit);
        assert_ne!(CollationSource::Explicit, CollationSource::Schema);
        assert_ne!(CollationSource::Schema, CollationSource::Default);
    }
}
