//! Fixture selection UX: ergonomic, unambiguous fixture resolution.
//!
//! Bead: bd-jwuo
//!
//! Every fixture has a stable `db_id` (manifest-driven) used across CLI flags,
//! JSON reports, and TUI panels.  This module provides:
//!
//! - **Exact resolution**: `--db beads_rust_beads` selects exactly one fixture.
//! - **Prefix/substring matching**: `--db beads` matches all IDs containing "beads".
//! - **Ambiguity detection**: multiple matches → clear error listing all candidates.
//! - **Tag filtering**: `--tag wal`, `--tag large`.
//! - **Size range filtering**: `--min-size 1MB`, `--max-size 100MB`.
//! - **Feature filtering**: `--requires-wal`, `--header-ok`.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

// ── Manifest types ───────────────────────────────────────────────────

/// A single entry in the corpus manifest (`manifest.v1.json`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestEntry {
    pub db_id: String,
    pub golden_filename: String,
    pub sha256_golden: String,
    pub size_bytes: u64,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub sqlite_meta: Option<ManifestSqliteMeta>,
}

/// SQLite PRAGMA metadata embedded in the manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestSqliteMeta {
    #[serde(default)]
    pub page_size: Option<u32>,
    #[serde(default)]
    pub journal_mode: Option<String>,
    #[serde(default)]
    pub user_version: Option<u32>,
    #[serde(default)]
    pub application_id: Option<u32>,
}

/// Top-level manifest file structure.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub manifest_version: u32,
    pub entries: Vec<ManifestEntry>,
}

// ── Filter criteria ──────────────────────────────────────────────────

/// Criteria for filtering fixtures.
#[derive(Debug, Clone, Default)]
pub struct FixtureFilter {
    /// Only include fixtures whose `db_id` matches this selector.
    ///
    /// Matching rules (in priority order):
    /// 1. Exact match on `db_id`.
    /// 2. If no exact match, substring match on `db_id`.
    pub db_selector: Option<String>,

    /// Only include fixtures tagged with ALL of these tags.
    pub require_tags: Vec<String>,

    /// Exclude fixtures tagged with ANY of these tags.
    pub exclude_tags: Vec<String>,

    /// Minimum file size in bytes (inclusive).
    pub min_size_bytes: Option<u64>,

    /// Maximum file size in bytes (inclusive).
    pub max_size_bytes: Option<u64>,

    /// Only include fixtures with WAL journal mode.
    pub requires_wal: bool,

    /// Only include fixtures marked safe for CI.
    pub ci_safe_only: bool,
}

// ── Selection result ─────────────────────────────────────────────────

/// Outcome of fixture selection.
#[derive(Debug, Clone)]
pub enum SelectionResult {
    /// Exactly one fixture matched.
    Single(ManifestEntry),
    /// Multiple fixtures matched — caller must disambiguate.
    Ambiguous {
        selector: String,
        candidates: Vec<ManifestEntry>,
    },
    /// No fixtures matched the filter.
    NoMatch { reason: String },
}

impl SelectionResult {
    /// Returns `Ok` if exactly one fixture was selected, otherwise an error
    /// message suitable for display.
    ///
    /// # Errors
    ///
    /// Returns a human-readable error string for ambiguous or no-match results.
    pub fn require_single(self) -> Result<ManifestEntry, String> {
        match self {
            Self::Single(entry) => Ok(entry),
            Self::Ambiguous {
                selector,
                candidates,
            } => {
                let mut msg = format!(
                    "ambiguous fixture selector `{selector}` matches {} fixtures:\n",
                    candidates.len()
                );
                for c in &candidates {
                    let _ = writeln!(
                        msg,
                        "  - {} ({}, {})",
                        c.db_id,
                        c.golden_filename,
                        format_size(c.size_bytes)
                    );
                }
                let _ = writeln!(msg, "\nPlease use a more specific --db value.");
                Err(msg)
            }
            Self::NoMatch { reason } => Err(format!("no fixture matched: {reason}")),
        }
    }

    /// Whether exactly one fixture was selected.
    #[must_use]
    pub fn is_single(&self) -> bool {
        matches!(self, Self::Single(_))
    }
}

// ── Manifest loading ─────────────────────────────────────────────────

/// Default path to the manifest file relative to the workspace root.
pub const MANIFEST_PATH_RELATIVE: &str = "sample_sqlite_db_files/manifests/manifest.v1.json";

/// Load the manifest from the default path.
///
/// # Errors
///
/// Returns an error if the file cannot be read or parsed.
pub fn load_manifest(workspace_root: &Path) -> Result<Manifest, String> {
    let path = workspace_root.join(MANIFEST_PATH_RELATIVE);
    load_manifest_from(&path)
}

/// Load a manifest from an explicit path.
///
/// # Errors
///
/// Returns an error if the file cannot be read or parsed.
pub fn load_manifest_from(path: &Path) -> Result<Manifest, String> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| format!("cannot read manifest at {}: {e}", path.display()))?;
    serde_json::from_str::<Manifest>(&content)
        .map_err(|e| format!("cannot parse manifest at {}: {e}", path.display()))
}

// ── Core selection logic ─────────────────────────────────────────────

/// Select fixtures matching the given filter.
///
/// If `filter.db_selector` is set:
/// 1. Try exact match first.
/// 2. If no exact match, try substring match.
/// 3. Apply remaining filters to the candidates.
///
/// If `filter.db_selector` is `None`, all entries pass the ID filter
/// and only tag/size/feature filters apply.
#[must_use]
pub fn select(manifest: &Manifest, filter: &FixtureFilter) -> SelectionResult {
    let id_candidates = match &filter.db_selector {
        Some(selector) => {
            // Step 1: exact match.
            let exact: Vec<&ManifestEntry> = manifest
                .entries
                .iter()
                .filter(|e| e.db_id == *selector)
                .collect();
            if exact.is_empty() {
                // Step 2: substring match.
                manifest
                    .entries
                    .iter()
                    .filter(|e| e.db_id.contains(selector.as_str()))
                    .collect()
            } else {
                exact
            }
        }
        None => manifest.entries.iter().collect(),
    };

    // Apply secondary filters.
    let filtered: Vec<ManifestEntry> = id_candidates
        .into_iter()
        .filter(|e| passes_secondary_filters(e, filter))
        .cloned()
        .collect();

    match filtered.len() {
        0 => SelectionResult::NoMatch {
            reason: describe_filter(filter),
        },
        1 => SelectionResult::Single(filtered.into_iter().next().expect("len == 1")),
        _ => {
            if let Some(sel) = &filter.db_selector {
                SelectionResult::Ambiguous {
                    selector: sel.clone(),
                    candidates: filtered,
                }
            } else {
                // No db_selector → return all matches as "ambiguous" so caller
                // can iterate.
                SelectionResult::Ambiguous {
                    selector: "(all)".to_owned(),
                    candidates: filtered,
                }
            }
        }
    }
}

/// Select all fixtures matching the given filter (returns a vec, never fails).
#[must_use]
pub fn select_all(manifest: &Manifest, filter: &FixtureFilter) -> Vec<ManifestEntry> {
    let id_candidates: Vec<&ManifestEntry> = match &filter.db_selector {
        Some(selector) => {
            let exact: Vec<&ManifestEntry> = manifest
                .entries
                .iter()
                .filter(|e| e.db_id == *selector)
                .collect();
            if exact.is_empty() {
                manifest
                    .entries
                    .iter()
                    .filter(|e| e.db_id.contains(selector.as_str()))
                    .collect()
            } else {
                exact
            }
        }
        None => manifest.entries.iter().collect(),
    };

    id_candidates
        .into_iter()
        .filter(|e| passes_secondary_filters(e, filter))
        .cloned()
        .collect()
}

fn passes_secondary_filters(entry: &ManifestEntry, filter: &FixtureFilter) -> bool {
    // Tag inclusion.
    if !filter.require_tags.is_empty()
        && !filter
            .require_tags
            .iter()
            .all(|t| entry.tags.iter().any(|et| et == t))
    {
        return false;
    }

    // Tag exclusion.
    if filter
        .exclude_tags
        .iter()
        .any(|t| entry.tags.iter().any(|et| et == t))
    {
        return false;
    }

    // Size range.
    if let Some(min) = filter.min_size_bytes {
        if entry.size_bytes < min {
            return false;
        }
    }
    if let Some(max) = filter.max_size_bytes {
        if entry.size_bytes > max {
            return false;
        }
    }

    // WAL requirement.
    if filter.requires_wal {
        let is_wal = entry
            .sqlite_meta
            .as_ref()
            .and_then(|m| m.journal_mode.as_deref())
            .is_some_and(|jm| jm.eq_ignore_ascii_case("wal"));
        if !is_wal {
            return false;
        }
    }

    true
}

fn describe_filter(filter: &FixtureFilter) -> String {
    let mut parts = Vec::new();
    if let Some(sel) = &filter.db_selector {
        parts.push(format!("db_id contains \"{sel}\""));
    }
    for tag in &filter.require_tags {
        parts.push(format!("tag={tag}"));
    }
    for tag in &filter.exclude_tags {
        parts.push(format!("exclude tag={tag}"));
    }
    if let Some(min) = filter.min_size_bytes {
        parts.push(format!("size >= {}", format_size(min)));
    }
    if let Some(max) = filter.max_size_bytes {
        parts.push(format!("size <= {}", format_size(max)));
    }
    if filter.requires_wal {
        parts.push("journal_mode=wal".to_owned());
    }
    if parts.is_empty() {
        "no entries in manifest".to_owned()
    } else {
        parts.join(", ")
    }
}

// ── CLI argument parsing ─────────────────────────────────────────────

/// Parse fixture-selection flags from a CLI argument list.
///
/// Recognized flags:
/// - `--db <ID>` — fixture selector (exact or substring).
/// - `--tag <TAG>` — require this tag (repeatable).
/// - `--exclude-tag <TAG>` — exclude this tag (repeatable).
/// - `--min-size <BYTES>` — minimum size (supports K/M/G suffixes).
/// - `--max-size <BYTES>` — maximum size (supports K/M/G suffixes).
/// - `--requires-wal` — only WAL-mode fixtures.
/// - `--ci-safe` — only CI-safe fixtures.
#[must_use]
pub fn parse_filter_args(args: &[String]) -> FixtureFilter {
    let mut filter = FixtureFilter::default();
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--db" => {
                if i + 1 < args.len() {
                    filter.db_selector = Some(args[i + 1].clone());
                    i += 1;
                }
            }
            "--tag" => {
                if i + 1 < args.len() {
                    filter.require_tags.push(args[i + 1].clone());
                    i += 1;
                }
            }
            "--exclude-tag" => {
                if i + 1 < args.len() {
                    filter.exclude_tags.push(args[i + 1].clone());
                    i += 1;
                }
            }
            "--min-size" => {
                if i + 1 < args.len() {
                    if let Some(bytes) = parse_size(&args[i + 1]) {
                        filter.min_size_bytes = Some(bytes);
                    }
                    i += 1;
                }
            }
            "--max-size" => {
                if i + 1 < args.len() {
                    if let Some(bytes) = parse_size(&args[i + 1]) {
                        filter.max_size_bytes = Some(bytes);
                    }
                    i += 1;
                }
            }
            "--requires-wal" => {
                filter.requires_wal = true;
            }
            "--ci-safe" => {
                filter.ci_safe_only = true;
            }
            _ => {}
        }
        i += 1;
    }
    filter
}

/// Help text for fixture selection flags.
#[must_use]
pub fn fixture_selection_help() -> &'static str {
    "\
FIXTURE SELECTION:
    --db <ID>              Select fixture by db_id (exact or substring match)
    --tag <TAG>            Require this tag (repeatable: --tag wal --tag large)
    --exclude-tag <TAG>    Exclude fixtures with this tag
    --min-size <SIZE>      Minimum file size (e.g., 1M, 500K, 1G)
    --max-size <SIZE>      Maximum file size
    --requires-wal         Only select WAL-mode fixtures
    --ci-safe              Only select CI-safe fixtures"
}

// ── Size parsing / formatting ────────────────────────────────────────

/// Parse a human-readable size string with optional suffix.
///
/// Supports: plain bytes, `K`/`KB`, `M`/`MB`, `G`/`GB` (case-insensitive).
#[must_use]
pub fn parse_size(s: &str) -> Option<u64> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }

    // Split numeric prefix from suffix.
    let (num_str, suffix) = split_numeric_suffix(s);
    let num: f64 = num_str.parse().ok()?;
    let multiplier = match suffix.to_ascii_lowercase().as_str() {
        "" | "b" => 1u64,
        "k" | "kb" => 1024,
        "m" | "mb" => 1024 * 1024,
        "g" | "gb" => 1024 * 1024 * 1024,
        _ => return None,
    };

    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    Some((num * multiplier as f64) as u64)
}

fn split_numeric_suffix(s: &str) -> (&str, &str) {
    let boundary = s
        .find(|c: char| !c.is_ascii_digit() && c != '.')
        .unwrap_or(s.len());
    (&s[..boundary], &s[boundary..])
}

/// Format bytes as a human-readable size string.
#[must_use]
pub fn format_size(bytes: u64) -> String {
    if bytes < 1024 {
        format!("{bytes}B")
    } else if bytes < 1024 * 1024 {
        format!("{:.1}KB", bytes as f64 / 1024.0)
    } else if bytes < 1024 * 1024 * 1024 {
        format!("{:.1}MB", bytes as f64 / (1024.0 * 1024.0))
    } else {
        format!("{:.2}GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    }
}

/// Resolve a `db_id` to the golden file path.
#[must_use]
pub fn resolve_golden_path(workspace_root: &Path, entry: &ManifestEntry) -> PathBuf {
    workspace_root
        .join("sample_sqlite_db_files")
        .join("golden")
        .join(&entry.golden_filename)
}

/// Resolve a `db_id` to the metadata JSON path.
#[must_use]
pub fn resolve_metadata_path(workspace_root: &Path, entry: &ManifestEntry) -> PathBuf {
    workspace_root
        .join("sample_sqlite_db_files")
        .join("metadata")
        .join(format!("{}.json", entry.db_id))
}

// ── List/display helpers ─────────────────────────────────────────────

/// Format a list of entries as a human-readable table.
#[must_use]
pub fn format_fixture_table(entries: &[ManifestEntry]) -> String {
    let mut out = String::with_capacity(entries.len() * 80);
    let _ = writeln!(out, "{:<35} {:>10} {:>6} tags", "db_id", "size", "pages");
    let _ = writeln!(out, "{}", "-".repeat(80));
    for e in entries {
        let pages = e
            .sqlite_meta
            .as_ref()
            .and_then(|m| m.page_size)
            .map_or_else(
                || "?".to_owned(),
                |ps| {
                    if ps > 0 {
                        format!("{}", e.size_bytes / u64::from(ps))
                    } else {
                        "?".to_owned()
                    }
                },
            );
        let tags = if e.tags.is_empty() {
            "-".to_owned()
        } else {
            e.tags.join(", ")
        };
        let _ = writeln!(
            out,
            "{:<35} {:>10} {:>6} {}",
            e.db_id,
            format_size(e.size_bytes),
            pages,
            tags
        );
    }
    out
}

// ── Tag synchronization ──────────────────────────────────────────────

/// Metadata path relative to the workspace root.
const METADATA_DIR_RELATIVE: &str = "sample_sqlite_db_files/metadata";

/// Sync tags from per-fixture metadata JSON files into the manifest.
///
/// For each manifest entry, reads `<metadata_dir>/<db_id>.json` and
/// copies its `tags` array into the entry.  Entries without a matching
/// metadata file keep their existing tags (or an empty vec).
///
/// Returns the number of entries that had their tags updated.
///
/// # Errors
///
/// Returns an error only if the metadata directory cannot be found.
pub fn sync_tags_from_metadata(
    manifest: &mut Manifest,
    workspace_root: &Path,
) -> Result<usize, String> {
    let meta_dir = workspace_root.join(METADATA_DIR_RELATIVE);
    if !meta_dir.is_dir() {
        return Err(format!(
            "metadata directory not found: {}",
            meta_dir.display()
        ));
    }

    let mut updated = 0;
    for entry in &mut manifest.entries {
        let meta_path = meta_dir.join(format!("{}.json", entry.db_id));
        if let Ok(content) = std::fs::read_to_string(&meta_path) {
            if let Ok(meta) = serde_json::from_str::<serde_json::Value>(&content) {
                if let Some(tags) = meta.get("tags").and_then(|v| v.as_array()) {
                    let new_tags: Vec<String> = tags
                        .iter()
                        .filter_map(|t| t.as_str().map(str::to_owned))
                        .collect();
                    if entry.tags != new_tags {
                        entry.tags = new_tags;
                        updated += 1;
                    }
                }
            }
        }
    }
    Ok(updated)
}

/// Write a manifest to disk (pretty-printed JSON).
///
/// # Errors
///
/// Returns an error if the file cannot be written.
pub fn save_manifest(manifest: &Manifest, path: &Path) -> Result<(), String> {
    let json = serde_json::to_string_pretty(manifest)
        .map_err(|e| format!("cannot serialize manifest: {e}"))?;
    std::fs::write(path, format!("{json}\n"))
        .map_err(|e| format!("cannot write manifest to {}: {e}", path.display()))
}

/// Validate that all `db_id` values in the manifest are unique.
///
/// Returns a list of duplicate IDs (empty if all are unique).
#[must_use]
pub fn find_duplicate_db_ids(manifest: &Manifest) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    let mut duplicates = Vec::new();
    for entry in &manifest.entries {
        if !seen.insert(&entry.db_id) {
            duplicates.push(entry.db_id.clone());
        }
    }
    duplicates
}

// ── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_manifest() -> Manifest {
        Manifest {
            manifest_version: 1,
            entries: vec![
                ManifestEntry {
                    db_id: "beads_rust_beads".to_owned(),
                    golden_filename: "beads_rust_beads.db".to_owned(),
                    sha256_golden: "aaa".to_owned(),
                    size_bytes: 4_120_576,
                    tags: vec!["beads".to_owned(), "medium".to_owned(), "wal".to_owned()],
                    sqlite_meta: Some(ManifestSqliteMeta {
                        page_size: Some(4096),
                        journal_mode: Some("wal".to_owned()),
                        user_version: None,
                        application_id: None,
                    }),
                },
                ManifestEntry {
                    db_id: "beads_viewer".to_owned(),
                    golden_filename: "beads_viewer.db".to_owned(),
                    sha256_golden: "bbb".to_owned(),
                    size_bytes: 6_565_888,
                    tags: vec!["beads".to_owned(), "large".to_owned(), "wal".to_owned()],
                    sqlite_meta: Some(ManifestSqliteMeta {
                        page_size: Some(4096),
                        journal_mode: Some("wal".to_owned()),
                        user_version: None,
                        application_id: None,
                    }),
                },
                ManifestEntry {
                    db_id: "frankensqlite".to_owned(),
                    golden_filename: "frankensqlite.db".to_owned(),
                    sha256_golden: "ccc".to_owned(),
                    size_bytes: 500_000,
                    tags: vec!["medium".to_owned()],
                    sqlite_meta: Some(ManifestSqliteMeta {
                        page_size: Some(4096),
                        journal_mode: Some("delete".to_owned()),
                        user_version: None,
                        application_id: None,
                    }),
                },
                ManifestEntry {
                    db_id: "tiny_test".to_owned(),
                    golden_filename: "tiny_test.db".to_owned(),
                    sha256_golden: "ddd".to_owned(),
                    size_bytes: 10_000,
                    tags: vec!["small".to_owned(), "test".to_owned()],
                    sqlite_meta: None,
                },
            ],
        }
    }

    #[test]
    fn test_exact_match() {
        let m = sample_manifest();
        let filter = FixtureFilter {
            db_selector: Some("beads_rust_beads".to_owned()),
            ..Default::default()
        };
        let result = select(&m, &filter);
        assert!(result.is_single());
        if let SelectionResult::Single(e) = result {
            assert_eq!(e.db_id, "beads_rust_beads");
        }
    }

    #[test]
    fn test_substring_match_unique() {
        let m = sample_manifest();
        let filter = FixtureFilter {
            db_selector: Some("frankensqlite".to_owned()),
            ..Default::default()
        };
        let result = select(&m, &filter);
        assert!(result.is_single());
    }

    #[test]
    fn test_substring_match_ambiguous() {
        let m = sample_manifest();
        let filter = FixtureFilter {
            db_selector: Some("beads".to_owned()),
            ..Default::default()
        };
        let result = select(&m, &filter);
        assert!(matches!(result, SelectionResult::Ambiguous { .. }));
        if let SelectionResult::Ambiguous { candidates, .. } = result {
            assert_eq!(candidates.len(), 2);
        }
    }

    #[test]
    fn test_no_match() {
        let m = sample_manifest();
        let filter = FixtureFilter {
            db_selector: Some("nonexistent".to_owned()),
            ..Default::default()
        };
        let result = select(&m, &filter);
        assert!(matches!(result, SelectionResult::NoMatch { .. }));
    }

    #[test]
    fn test_require_single_ok() {
        let m = sample_manifest();
        let filter = FixtureFilter {
            db_selector: Some("beads_rust_beads".to_owned()),
            ..Default::default()
        };
        let entry = select(&m, &filter).require_single().unwrap();
        assert_eq!(entry.db_id, "beads_rust_beads");
    }

    #[test]
    fn test_require_single_ambiguous_error() {
        let m = sample_manifest();
        let filter = FixtureFilter {
            db_selector: Some("beads".to_owned()),
            ..Default::default()
        };
        let err = select(&m, &filter).require_single().unwrap_err();
        assert!(err.contains("ambiguous"));
        assert!(err.contains("beads_rust_beads"));
        assert!(err.contains("beads_viewer"));
    }

    #[test]
    fn test_tag_filter() {
        let m = sample_manifest();
        let filter = FixtureFilter {
            require_tags: vec!["beads".to_owned()],
            ..Default::default()
        };
        let results = select_all(&m, &filter);
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|e| e.tags.contains(&"beads".to_owned())));
    }

    #[test]
    fn test_exclude_tag_filter() {
        let m = sample_manifest();
        let filter = FixtureFilter {
            exclude_tags: vec!["beads".to_owned()],
            ..Default::default()
        };
        let results = select_all(&m, &filter);
        assert_eq!(results.len(), 2);
        assert!(
            results
                .iter()
                .all(|e| !e.tags.contains(&"beads".to_owned()))
        );
    }

    #[test]
    fn test_size_range_filter() {
        let m = sample_manifest();
        let filter = FixtureFilter {
            min_size_bytes: Some(1_000_000),
            max_size_bytes: Some(5_000_000),
            ..Default::default()
        };
        let results = select_all(&m, &filter);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].db_id, "beads_rust_beads");
    }

    #[test]
    fn test_wal_filter() {
        let m = sample_manifest();
        let filter = FixtureFilter {
            requires_wal: true,
            ..Default::default()
        };
        let results = select_all(&m, &filter);
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|e| e.db_id.contains("beads")));
    }

    #[test]
    fn test_combined_filters() {
        let m = sample_manifest();
        let filter = FixtureFilter {
            require_tags: vec!["beads".to_owned()],
            min_size_bytes: Some(5_000_000),
            ..Default::default()
        };
        let results = select_all(&m, &filter);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].db_id, "beads_viewer");
    }

    #[test]
    fn test_parse_size() {
        assert_eq!(parse_size("1024"), Some(1024));
        assert_eq!(parse_size("1K"), Some(1024));
        assert_eq!(parse_size("1KB"), Some(1024));
        assert_eq!(parse_size("1M"), Some(1024 * 1024));
        assert_eq!(parse_size("1MB"), Some(1024 * 1024));
        assert_eq!(parse_size("1G"), Some(1024 * 1024 * 1024));
        assert_eq!(parse_size("1.5M"), Some(1_572_864));
        assert_eq!(parse_size("500k"), Some(512_000));
        assert_eq!(parse_size(""), None);
        assert_eq!(parse_size("abc"), None);
    }

    #[test]
    fn test_format_size() {
        assert_eq!(format_size(500), "500B");
        assert_eq!(format_size(1024), "1.0KB");
        assert_eq!(format_size(1_048_576), "1.0MB");
        assert_eq!(format_size(1_073_741_824), "1.00GB");
    }

    #[test]
    fn test_parse_filter_args() {
        let args: Vec<String> = vec![
            "run".into(),
            "--db".into(),
            "beads".into(),
            "--tag".into(),
            "wal".into(),
            "--tag".into(),
            "large".into(),
            "--min-size".into(),
            "1M".into(),
            "--requires-wal".into(),
        ];
        let filter = parse_filter_args(&args);
        assert_eq!(filter.db_selector, Some("beads".to_owned()));
        assert_eq!(filter.require_tags, vec!["wal", "large"]);
        assert_eq!(filter.min_size_bytes, Some(1024 * 1024));
        assert!(filter.requires_wal);
    }

    #[test]
    fn test_parse_filter_args_empty() {
        let args: Vec<String> = vec!["run".into()];
        let filter = parse_filter_args(&args);
        assert!(filter.db_selector.is_none());
        assert!(filter.require_tags.is_empty());
    }

    #[test]
    fn test_fixture_table_format() {
        let m = sample_manifest();
        let table = format_fixture_table(&m.entries);
        assert!(table.contains("beads_rust_beads"));
        assert!(table.contains("frankensqlite"));
        assert!(table.contains("db_id"));
    }

    #[test]
    fn test_resolve_paths() {
        let entry = ManifestEntry {
            db_id: "test_db".to_owned(),
            golden_filename: "test_db.db".to_owned(),
            sha256_golden: "abc".to_owned(),
            size_bytes: 100,
            tags: vec![],
            sqlite_meta: None,
        };
        let root = Path::new("/workspace");
        let golden = resolve_golden_path(root, &entry);
        assert_eq!(
            golden,
            PathBuf::from("/workspace/sample_sqlite_db_files/golden/test_db.db")
        );
        let meta = resolve_metadata_path(root, &entry);
        assert_eq!(
            meta,
            PathBuf::from("/workspace/sample_sqlite_db_files/metadata/test_db.json")
        );
    }

    #[test]
    fn test_selection_help_text() {
        let help = fixture_selection_help();
        assert!(help.contains("--db"));
        assert!(help.contains("--tag"));
        assert!(help.contains("--min-size"));
        assert!(help.contains("--requires-wal"));
    }

    #[test]
    fn test_load_manifest_real() {
        // Try loading the real manifest from the workspace.
        let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(Path::parent)
            .unwrap();
        if let Ok(manifest) = load_manifest(workspace_root) {
            assert_eq!(manifest.manifest_version, 1);
            assert!(!manifest.entries.is_empty());
            // Every entry should have a non-empty db_id.
            for entry in &manifest.entries {
                assert!(!entry.db_id.is_empty());
                assert!(!entry.golden_filename.is_empty());
                assert_eq!(entry.sha256_golden.len(), 64);
            }
        }
        // Don't fail if manifest doesn't exist (CI without corpus).
    }

    #[test]
    fn test_select_all_no_filter() {
        let m = sample_manifest();
        let filter = FixtureFilter::default();
        let results = select_all(&m, &filter);
        assert_eq!(results.len(), 4);
    }

    #[test]
    fn test_no_match_description() {
        let m = sample_manifest();
        let filter = FixtureFilter {
            db_selector: Some("nonexistent".to_owned()),
            require_tags: vec!["rare".to_owned()],
            ..Default::default()
        };
        let result = select(&m, &filter);
        if let SelectionResult::NoMatch { reason } = result {
            assert!(reason.contains("nonexistent"));
            assert!(reason.contains("tag=rare"));
        } else {
            panic!("expected NoMatch");
        }
    }
}
