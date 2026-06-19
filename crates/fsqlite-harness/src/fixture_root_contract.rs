//! Canonical fixture-root contract loader for Track C corpus gates.
//!
//! This module enforces a single source of truth for fixture roots and
//! cardinality floors, rooted in `docs/contracts/corpus_manifest.toml`.

use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use sha2::{Digest, Sha256};

/// Default canonical fixture-root manifest path relative to workspace root.
pub const DEFAULT_FIXTURE_ROOT_MANIFEST_PATH: &str = "docs/contracts/corpus_manifest.toml";
/// Expected schema version for `[fixture_roots]`.
pub const FIXTURE_ROOT_SCHEMA_VERSION: &str = "1.0.0";
/// Deterministic directory-hash algorithm used by canonical fixture roots.
pub const FIXTURE_ROOT_TREE_HASH_ALGORITHM: &str = "sha256-tree-v1";

/// Loaded canonical fixture-root contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FixtureRootContract {
    /// Absolute manifest path.
    pub manifest_path: PathBuf,
    /// SHA-256 of the raw manifest payload.
    pub manifest_sha256: String,
    /// Absolute fixtures directory.
    pub fixtures_dir: PathBuf,
    /// Explicit fixture directory aliases accepted by runners.
    pub fixtures_dir_aliases: Vec<PathBuf>,
    /// Absolute SLT directory.
    pub slt_dir: PathBuf,
    /// Explicit SLT directory aliases accepted by runners.
    pub slt_dir_aliases: Vec<PathBuf>,
    /// Minimum fixture JSON files required.
    pub min_fixture_json_files: usize,
    /// Minimum fixture entries required.
    pub min_fixture_entries: usize,
    /// Minimum fixture SQL statements required.
    pub min_fixture_sql_statements: usize,
    /// Minimum SLT files required.
    pub min_slt_files: usize,
    /// Minimum SLT entries required.
    pub min_slt_entries: usize,
    /// Minimum SLT SQL statements required.
    pub min_slt_sql_statements: usize,
    /// Required category families that must exist in `category_floors`.
    pub required_category_families: Vec<String>,
    /// Hash-locked fixture roots validated from the manifest.
    pub hash_locked_roots: Vec<HashLockedFixtureRoot>,
}

/// Hash-locked fixture root entry from the canonical manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HashLockedFixtureRoot {
    /// Stable manifest identifier for first-failure diagnostics.
    pub root_id: String,
    /// Absolute root path.
    pub path: PathBuf,
    /// Human-readable root kind, such as `fixture_json`.
    pub kind: String,
    /// Hash algorithm used by `content_hash`.
    pub hash_algorithm: String,
    /// File extensions included in the hash lock.
    pub include_extensions: Vec<String>,
    /// Expected root content hash from the manifest.
    pub content_hash: String,
    /// Minimum included files required by this root lock.
    pub min_files: usize,
    /// Observed included file count at load time.
    pub file_count: usize,
}

#[derive(Debug, Deserialize)]
struct CorpusManifestDocument {
    fixture_roots: Option<FixtureRootsSection>,
    #[serde(default)]
    category_floors: Vec<CategoryFloorEntry>,
}

#[derive(Debug, Deserialize)]
struct FixtureRootsSection {
    schema_version: String,
    fixtures_dir: String,
    #[serde(default)]
    fixtures_dir_aliases: Vec<String>,
    slt_dir: String,
    #[serde(default)]
    slt_dir_aliases: Vec<String>,
    min_fixture_json_files: usize,
    min_fixture_entries: usize,
    min_fixture_sql_statements: usize,
    min_slt_files: usize,
    min_slt_entries: usize,
    min_slt_sql_statements: usize,
    #[serde(default)]
    required_category_families: Vec<String>,
    #[serde(default)]
    hash_locked_roots: Vec<HashLockedRootEntry>,
}

#[derive(Debug, Deserialize)]
struct HashLockedRootEntry {
    root_id: String,
    path: String,
    kind: String,
    hash_algorithm: String,
    #[serde(default)]
    include_extensions: Vec<String>,
    content_hash: String,
    min_files: usize,
}

#[derive(Debug, Deserialize)]
struct CategoryFloorEntry {
    category: String,
    min_entries: usize,
}

/// Load and validate the canonical fixture-root contract.
///
/// # Errors
///
/// Returns `Err` if the manifest is missing, malformed, or violates contract
/// requirements.
pub fn load_fixture_root_contract(
    workspace_root: &Path,
    manifest_path: &Path,
) -> Result<FixtureRootContract, String> {
    let manifest_path = resolve_workspace_path(workspace_root, manifest_path);
    if !manifest_path.is_file() {
        return Err(format!(
            "fixture_root_manifest_missing path={}",
            manifest_path.display()
        ));
    }

    let raw = fs::read_to_string(&manifest_path).map_err(|error| {
        format!(
            "fixture_root_manifest_read_failed path={} error={error}",
            manifest_path.display()
        )
    })?;
    if raw.trim().is_empty() {
        return Err(format!(
            "fixture_root_manifest_empty path={}",
            manifest_path.display()
        ));
    }

    let manifest_sha256 = sha256_hex(raw.as_bytes());
    let doc = toml::from_str::<CorpusManifestDocument>(&raw).map_err(|error| {
        format!(
            "fixture_root_manifest_parse_failed path={} error={error}",
            manifest_path.display()
        )
    })?;

    let section = doc.fixture_roots.ok_or_else(|| {
        format!(
            "fixture_root_manifest_missing_section path={} section=fixture_roots",
            manifest_path.display()
        )
    })?;

    if section.schema_version != FIXTURE_ROOT_SCHEMA_VERSION {
        return Err(format!(
            "fixture_root_schema_version_mismatch expected={} observed={}",
            FIXTURE_ROOT_SCHEMA_VERSION, section.schema_version
        ));
    }

    let fixtures_dir = non_empty_string("fixture_roots.fixtures_dir", &section.fixtures_dir)?;
    let slt_dir = non_empty_string("fixture_roots.slt_dir", &section.slt_dir)?;
    let fixtures_dir_aliases = normalize_path_aliases(
        "fixture_roots.fixtures_dir_aliases",
        workspace_root,
        section.fixtures_dir_aliases,
    )?;
    let slt_dir_aliases = normalize_path_aliases(
        "fixture_roots.slt_dir_aliases",
        workspace_root,
        section.slt_dir_aliases,
    )?;
    require_positive(
        "fixture_roots.min_fixture_json_files",
        section.min_fixture_json_files,
    )?;
    require_positive(
        "fixture_roots.min_fixture_entries",
        section.min_fixture_entries,
    )?;
    require_positive(
        "fixture_roots.min_fixture_sql_statements",
        section.min_fixture_sql_statements,
    )?;
    require_positive("fixture_roots.min_slt_files", section.min_slt_files)?;
    require_positive("fixture_roots.min_slt_entries", section.min_slt_entries)?;
    require_positive(
        "fixture_roots.min_slt_sql_statements",
        section.min_slt_sql_statements,
    )?;

    let required_category_families =
        normalize_required_categories(section.required_category_families)?;
    validate_required_categories(&required_category_families, &doc.category_floors)?;
    let hash_locked_roots = validate_hash_locked_roots(workspace_root, section.hash_locked_roots)?;

    Ok(FixtureRootContract {
        manifest_path,
        manifest_sha256,
        fixtures_dir: resolve_workspace_path(workspace_root, Path::new(&fixtures_dir)),
        fixtures_dir_aliases,
        slt_dir: resolve_workspace_path(workspace_root, Path::new(&slt_dir)),
        slt_dir_aliases,
        min_fixture_json_files: section.min_fixture_json_files,
        min_fixture_entries: section.min_fixture_entries,
        min_fixture_sql_statements: section.min_fixture_sql_statements,
        min_slt_files: section.min_slt_files,
        min_slt_entries: section.min_slt_entries,
        min_slt_sql_statements: section.min_slt_sql_statements,
        required_category_families,
        hash_locked_roots,
    })
}

fn validate_hash_locked_roots(
    workspace_root: &Path,
    roots: Vec<HashLockedRootEntry>,
) -> Result<Vec<HashLockedFixtureRoot>, String> {
    let mut seen = BTreeSet::new();
    let mut validated = Vec::with_capacity(roots.len());

    for root in roots {
        let root_id = non_empty_string("fixture_roots.hash_locked_roots.root_id", &root.root_id)?;
        if !seen.insert(root_id.clone()) {
            return Err(format!(
                "fixture_root_hash_lock_duplicate root_id={root_id}"
            ));
        }
        let path_text = non_empty_string("fixture_roots.hash_locked_roots.path", &root.path)?;
        let kind = non_empty_string("fixture_roots.hash_locked_roots.kind", &root.kind)?;
        let hash_algorithm = non_empty_string(
            "fixture_roots.hash_locked_roots.hash_algorithm",
            &root.hash_algorithm,
        )?;
        if hash_algorithm != FIXTURE_ROOT_TREE_HASH_ALGORITHM {
            return Err(format!(
                "fixture_root_hash_lock_unknown_algorithm root_id={} algorithm={} expected={}",
                root_id, hash_algorithm, FIXTURE_ROOT_TREE_HASH_ALGORITHM
            ));
        }
        let content_hash = normalize_sha256_hex(
            "fixture_roots.hash_locked_roots.content_hash",
            &root.content_hash,
        )?;
        require_positive("fixture_roots.hash_locked_roots.min_files", root.min_files)?;
        let include_extensions = normalize_include_extensions(root.include_extensions)?;
        let path = resolve_workspace_path(workspace_root, Path::new(&path_text));
        if !path.is_dir() {
            return Err(format!(
                "fixture_root_hash_lock_missing_dir root_id={} path={}",
                root_id,
                path.display()
            ));
        }

        let (observed_hash, file_count) = hash_fixture_root_tree(&path, &include_extensions)?;
        if file_count < root.min_files {
            return Err(format!(
                "fixture_root_hash_lock_cardinality_failed root_id={} path={} files_seen={} min_files={}",
                root_id,
                path.display(),
                file_count,
                root.min_files
            ));
        }
        if observed_hash != content_hash {
            return Err(format!(
                "fixture_root_hash_lock_mismatch root_id={} path={} expected={} observed={} files_seen={}",
                root_id,
                path.display(),
                content_hash,
                observed_hash,
                file_count
            ));
        }

        validated.push(HashLockedFixtureRoot {
            root_id,
            path,
            kind,
            hash_algorithm,
            include_extensions,
            content_hash,
            min_files: root.min_files,
            file_count,
        });
    }

    Ok(validated)
}

fn hash_fixture_root_tree(
    root: &Path,
    include_extensions: &[String],
) -> Result<(String, usize), String> {
    let mut files = Vec::new();
    collect_hash_locked_files(root, include_extensions, &mut files)?;
    files.sort();

    let mut payload = String::new();
    for path in &files {
        let bytes = fs::read(path).map_err(|error| {
            format!(
                "fixture_root_hash_lock_read_failed path={} error={error}",
                path.display()
            )
        })?;
        let rel_path = path.strip_prefix(root).map_err(|error| {
            format!(
                "fixture_root_hash_lock_strip_prefix_failed root={} path={} error={error}",
                root.display(),
                path.display()
            )
        })?;
        let rel_path = rel_path.to_string_lossy().replace('\\', "/");
        let file_hash = sha256_hex(&bytes);
        writeln!(payload, "{rel_path}\t{}\t{file_hash}", bytes.len())
            .map_err(|error| format!("fixture_root_hash_lock_payload_failed: {error}"))?;
    }

    Ok((sha256_hex(payload.as_bytes()), files.len()))
}

fn collect_hash_locked_files(
    dir: &Path,
    include_extensions: &[String],
    out: &mut Vec<PathBuf>,
) -> Result<(), String> {
    let entries = fs::read_dir(dir).map_err(|error| {
        format!(
            "fixture_root_hash_lock_read_dir_failed path={} error={error}",
            dir.display()
        )
    })?;
    for entry in entries {
        let entry = entry.map_err(|error| {
            format!(
                "fixture_root_hash_lock_read_dir_entry_failed path={} error={error}",
                dir.display()
            )
        })?;
        let path = entry.path();
        let file_type = entry.file_type().map_err(|error| {
            format!(
                "fixture_root_hash_lock_file_type_failed path={} error={error}",
                path.display()
            )
        })?;
        if file_type.is_dir() {
            collect_hash_locked_files(&path, include_extensions, out)?;
        } else if file_type.is_file() && extension_matches(&path, include_extensions) {
            out.push(path);
        }
    }
    Ok(())
}

fn extension_matches(path: &Path, include_extensions: &[String]) -> bool {
    path.extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| {
            include_extensions
                .iter()
                .any(|expected| extension.eq_ignore_ascii_case(expected))
        })
}

/// Enforce runtime fixture/slt settings against the canonical contract.
///
/// # Errors
///
/// Returns `Err` when any path/threshold differs from canonical values.
#[allow(clippy::too_many_arguments)]
pub fn enforce_fixture_contract_alignment(
    contract: &FixtureRootContract,
    fixtures_dir: &Path,
    slt_dir: &Path,
    min_fixture_json_files: usize,
    min_fixture_entries: usize,
    min_fixture_sql_statements: usize,
    min_slt_files: usize,
    min_slt_entries: usize,
    min_slt_sql_statements: usize,
) -> Result<(), String> {
    let mut mismatches = Vec::new();
    if !matches_path_or_alias(
        fixtures_dir,
        &contract.fixtures_dir,
        &contract.fixtures_dir_aliases,
    ) {
        mismatches.push(format!(
            "fixtures_dir mismatch expected={} observed={}",
            contract.fixtures_dir.display(),
            fixtures_dir.display()
        ));
    }
    if !matches_path_or_alias(slt_dir, &contract.slt_dir, &contract.slt_dir_aliases) {
        mismatches.push(format!(
            "slt_dir mismatch expected={} observed={}",
            contract.slt_dir.display(),
            slt_dir.display()
        ));
    }
    if min_fixture_json_files != contract.min_fixture_json_files {
        mismatches.push(format!(
            "min_fixture_json_files mismatch expected={} observed={}",
            contract.min_fixture_json_files, min_fixture_json_files
        ));
    }
    if min_fixture_entries != contract.min_fixture_entries {
        mismatches.push(format!(
            "min_fixture_entries mismatch expected={} observed={}",
            contract.min_fixture_entries, min_fixture_entries
        ));
    }
    if min_fixture_sql_statements != contract.min_fixture_sql_statements {
        mismatches.push(format!(
            "min_fixture_sql_statements mismatch expected={} observed={}",
            contract.min_fixture_sql_statements, min_fixture_sql_statements
        ));
    }
    if min_slt_files != contract.min_slt_files {
        mismatches.push(format!(
            "min_slt_files mismatch expected={} observed={}",
            contract.min_slt_files, min_slt_files
        ));
    }
    if min_slt_entries != contract.min_slt_entries {
        mismatches.push(format!(
            "min_slt_entries mismatch expected={} observed={}",
            contract.min_slt_entries, min_slt_entries
        ));
    }
    if min_slt_sql_statements != contract.min_slt_sql_statements {
        mismatches.push(format!(
            "min_slt_sql_statements mismatch expected={} observed={}",
            contract.min_slt_sql_statements, min_slt_sql_statements
        ));
    }

    if mismatches.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "fixture_root_contract_alignment_failed manifest={} {}",
            contract.manifest_path.display(),
            mismatches.join("; ")
        ))
    }
}

fn validate_required_categories(
    required_category_families: &[String],
    category_floors: &[CategoryFloorEntry],
) -> Result<(), String> {
    let mut categories = BTreeSet::new();
    for floor in category_floors {
        if floor.min_entries == 0 {
            return Err(format!(
                "fixture_root_manifest_invalid_category_floor category={} min_entries=0",
                floor.category
            ));
        }
        categories.insert(floor.category.trim().to_owned());
    }

    let mut missing = Vec::new();
    for required in required_category_families {
        if !categories.contains(required) {
            missing.push(required.clone());
        }
    }

    if missing.is_empty() {
        Ok(())
    } else {
        missing.sort();
        Err(format!(
            "fixture_root_manifest_missing_required_category_floors missing={}",
            missing.join(",")
        ))
    }
}

fn normalize_required_categories(values: Vec<String>) -> Result<Vec<String>, String> {
    let mut categories = values
        .into_iter()
        .map(|value| value.trim().to_owned())
        .collect::<Vec<_>>();
    categories.retain(|value| !value.is_empty());
    if categories.is_empty() {
        return Err(
            "fixture_root_manifest_required_category_families_must_not_be_empty".to_owned(),
        );
    }

    let mut unique = BTreeSet::new();
    let mut normalized = Vec::with_capacity(categories.len());
    for category in categories {
        if unique.insert(category.clone()) {
            normalized.push(category);
        }
    }
    Ok(normalized)
}

fn normalize_path_aliases(
    field_name: &str,
    workspace_root: &Path,
    values: Vec<String>,
) -> Result<Vec<PathBuf>, String> {
    if values.is_empty() {
        return Err(format!("{field_name} must be non-empty"));
    }

    let mut aliases = Vec::with_capacity(values.len());
    let mut seen = BTreeSet::new();
    for value in values {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return Err(format!("{field_name} contains empty value"));
        }
        if seen.insert(trimmed.to_owned()) {
            aliases.push(resolve_workspace_path(workspace_root, Path::new(trimmed)));
        }
    }
    Ok(aliases)
}

fn normalize_include_extensions(values: Vec<String>) -> Result<Vec<String>, String> {
    if values.is_empty() {
        return Err(
            "fixture_roots.hash_locked_roots.include_extensions must be non-empty".to_owned(),
        );
    }

    let mut seen = BTreeSet::new();
    let mut normalized = Vec::with_capacity(values.len());
    for value in values {
        let trimmed = value.trim().trim_start_matches('.');
        if trimmed.is_empty() {
            return Err(
                "fixture_roots.hash_locked_roots.include_extensions contains empty value"
                    .to_owned(),
            );
        }
        let extension = trimmed.to_ascii_lowercase();
        if seen.insert(extension.clone()) {
            normalized.push(extension);
        }
    }
    Ok(normalized)
}

fn normalize_sha256_hex(field_name: &str, value: &str) -> Result<String, String> {
    let trimmed = value.trim();
    if trimmed.len() != 64 || !trimmed.chars().all(|ch| ch.is_ascii_hexdigit()) {
        return Err(format!(
            "{field_name} must be a 64-character SHA-256 hex digest"
        ));
    }
    Ok(trimmed.to_ascii_lowercase())
}

fn non_empty_string(field_name: &str, value: &str) -> Result<String, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        Err(format!("{field_name} must be non-empty"))
    } else {
        Ok(trimmed.to_owned())
    }
}

fn require_positive(field_name: &str, value: usize) -> Result<(), String> {
    if value == 0 {
        Err(format!("{field_name} must be > 0"))
    } else {
        Ok(())
    }
}

fn same_path(left: &Path, right: &Path) -> bool {
    match (left.canonicalize(), right.canonicalize()) {
        (Ok(left_path), Ok(right_path)) => left_path == right_path,
        _ => left == right,
    }
}

fn matches_path_or_alias(path: &Path, canonical: &Path, aliases: &[PathBuf]) -> bool {
    same_path(path, canonical) || aliases.iter().any(|alias| same_path(path, alias))
}

fn resolve_workspace_path(workspace_root: &Path, path: &Path) -> PathBuf {
    if path.is_relative() {
        workspace_root.join(path)
    } else {
        path.to_path_buf()
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    crate::bytes_to_lower_hex(digest)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn write_minimal_manifest(workspace_root: &Path, content_hash: &str) -> PathBuf {
        let manifest_path = workspace_root.join("corpus_manifest.toml");
        fs::write(
            &manifest_path,
            format!(
                r#"[fixture_roots]
schema_version = "1.0.0"
fixtures_dir = "fixtures"
fixtures_dir_aliases = ["./fixtures"]
slt_dir = "slt"
slt_dir_aliases = ["./slt"]
min_fixture_json_files = 1
min_fixture_entries = 1
min_fixture_sql_statements = 1
min_slt_files = 1
min_slt_entries = 1
min_slt_sql_statements = 1
required_category_families = ["ddl"]

[[fixture_roots.hash_locked_roots]]
root_id = "unit-fixtures"
path = "fixtures"
kind = "fixture_json"
hash_algorithm = "sha256-tree-v1"
include_extensions = ["json"]
content_hash = "{content_hash}"
min_files = 1

[[category_floors]]
category = "ddl"
min_entries = 1
"#
            ),
        )
        .expect("write manifest");
        manifest_path
    }

    #[test]
    fn hash_locked_root_accepts_matching_tree() {
        let workspace = TempDir::new().expect("tempdir");
        let fixtures = workspace.path().join("fixtures");
        fs::create_dir(&fixtures).expect("create fixtures");
        fs::create_dir(workspace.path().join("slt")).expect("create slt");
        fs::write(fixtures.join("a.json"), br#"{"ops":[{"sql":"SELECT 1"}]}"#)
            .expect("write fixture");

        let (hash, _) =
            hash_fixture_root_tree(&fixtures, &["json".to_owned()]).expect("hash fixture root");
        let manifest_path = write_minimal_manifest(workspace.path(), &hash);

        let contract = load_fixture_root_contract(workspace.path(), &manifest_path)
            .expect("matching hash lock should load");
        assert_eq!(contract.hash_locked_roots.len(), 1);
        assert_eq!(contract.hash_locked_roots[0].root_id, "unit-fixtures");
        assert_eq!(contract.hash_locked_roots[0].file_count, 1);
    }

    #[test]
    fn hash_locked_root_reports_mismatch_with_root_id() {
        let workspace = TempDir::new().expect("tempdir");
        let fixtures = workspace.path().join("fixtures");
        fs::create_dir(&fixtures).expect("create fixtures");
        fs::create_dir(workspace.path().join("slt")).expect("create slt");
        let fixture_path = fixtures.join("a.json");
        fs::write(&fixture_path, br#"{"ops":[{"sql":"SELECT 1"}]}"#).expect("write fixture");

        let (hash, _) =
            hash_fixture_root_tree(&fixtures, &["json".to_owned()]).expect("hash fixture root");
        let manifest_path = write_minimal_manifest(workspace.path(), &hash);
        fs::write(&fixture_path, br#"{"ops":[{"sql":"SELECT 2"}]}"#).expect("mutate fixture");

        let error = load_fixture_root_contract(workspace.path(), &manifest_path)
            .expect_err("mutated hash lock should fail");
        assert!(error.contains("fixture_root_hash_lock_mismatch"));
        assert!(error.contains("root_id=unit-fixtures"));
    }
}
