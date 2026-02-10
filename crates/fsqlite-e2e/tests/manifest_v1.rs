//! Validate that the tracked corpus manifest is present and well-formed.
//!
//! This test is CI-friendly: it does NOT require the golden `.db` binaries to
//! be present (they are git-ignored). It only reads git-tracked artifacts.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct ManifestV1 {
    manifest_version: u32,
    #[allow(dead_code)]
    generated_at: Option<String>,
    entries: Vec<ManifestEntryV1>,
    #[allow(dead_code)]
    notes: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ManifestEntryV1 {
    db_id: String,
    golden_filename: String,
    #[allow(dead_code)]
    source_path: Option<String>,
    #[allow(dead_code)]
    provenance: Option<String>,
    sha256_golden: String,
    #[allow(dead_code)]
    size_bytes: u64,
    sqlite_meta: Option<SqliteMeta>,
    #[allow(dead_code)]
    tags: Option<Vec<String>>,
    #[allow(dead_code)]
    safety: Option<SafetyMeta>,
    #[allow(dead_code)]
    notes: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SqliteMeta {
    page_size: Option<u32>,
    #[allow(dead_code)]
    encoding: Option<String>,
    #[allow(dead_code)]
    user_version: Option<u32>,
    #[allow(dead_code)]
    application_id: Option<u32>,
    #[allow(dead_code)]
    journal_mode: Option<String>,
    #[allow(dead_code)]
    auto_vacuum: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct SafetyMeta {
    #[allow(dead_code)]
    pii_risk: Option<String>,
}

fn workspace_root() -> PathBuf {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest
        .parent()
        .and_then(std::path::Path::parent)
        .expect("workspace root")
        .to_path_buf()
}

fn corpus_path(relative: &str) -> PathBuf {
    workspace_root().join(relative)
}

fn is_db_id_first(b: u8) -> bool {
    matches!(b, b'a'..=b'z' | b'0'..=b'9')
}

fn is_db_id_rest(b: u8) -> bool {
    matches!(b, b'a'..=b'z' | b'0'..=b'9' | b'_' | b'-')
}

fn valid_db_id(db_id: &str) -> bool {
    // Pattern in schema: ^[a-z0-9][a-z0-9_\-]{1,63}$
    let bytes = db_id.as_bytes();
    if bytes.len() < 2 || bytes.len() > 64 {
        return false;
    }
    if !is_db_id_first(bytes[0]) {
        return false;
    }
    bytes[1..].iter().copied().all(is_db_id_rest)
}

fn valid_sha256_hex_lower(s: &str) -> bool {
    if s.len() != 64 {
        return false;
    }
    s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

#[test]
fn manifest_v1_exists_and_is_consistent() {
    let manifest_path = corpus_path("sample_sqlite_db_files/manifests/manifest.v1.json");
    assert!(
        manifest_path.exists(),
        "manifest file must exist at {}",
        manifest_path.display()
    );

    let manifest_raw = std::fs::read_to_string(&manifest_path).expect("read manifest.v1.json");
    let manifest: ManifestV1 = serde_json::from_str(&manifest_raw).expect("parse manifest.v1.json");

    assert_eq!(manifest.manifest_version, 1, "manifest_version must be 1");

    assert!(
        manifest.entries.len() >= 10,
        "expected at least 10 fixtures in manifest (got {})",
        manifest.entries.len()
    );

    // Enforce deterministic ordering: sorted by db_id.
    let ids: Vec<&str> = manifest.entries.iter().map(|e| e.db_id.as_str()).collect();
    let mut sorted = ids.clone();
    sorted.sort_unstable();
    assert_eq!(ids, sorted, "manifest entries must be sorted by db_id");

    let mut seen_ids: HashSet<&str> = HashSet::new();
    let mut seen_filenames: HashSet<&str> = HashSet::new();

    for entry in &manifest.entries {
        assert!(
            valid_db_id(&entry.db_id),
            "invalid db_id (schema pattern mismatch): {}",
            entry.db_id
        );
        assert!(
            seen_ids.insert(entry.db_id.as_str()),
            "duplicate db_id in manifest: {}",
            entry.db_id
        );
        assert!(
            seen_filenames.insert(entry.golden_filename.as_str()),
            "duplicate golden_filename in manifest: {}",
            entry.golden_filename
        );

        assert!(
            !entry.golden_filename.is_empty(),
            "golden_filename must be non-empty for {}",
            entry.db_id
        );
        assert!(
            !entry.golden_filename.contains('/') && !entry.golden_filename.contains('\\'),
            "golden_filename must be a file name, not a path: {}",
            entry.golden_filename
        );

        assert!(
            valid_sha256_hex_lower(&entry.sha256_golden),
            "sha256_golden must be lowercase 64-hex for {}",
            entry.db_id
        );

        assert_eq!(
            Path::new(&entry.golden_filename)
                .file_stem()
                .expect("golden_filename must have a stem")
                .to_string_lossy(),
            entry.db_id,
            "db_id must match golden_filename stem"
        );

        assert!(
            entry.size_bytes > 0,
            "size_bytes must be > 0 for {}",
            entry.db_id
        );

        // Acceptance requires the manifest to capture page_size.
        let sqlite_meta = entry
            .sqlite_meta
            .as_ref()
            .unwrap_or_else(|| panic!("manifest entry {} missing sqlite_meta", entry.db_id));
        assert!(
            sqlite_meta.page_size.is_some(),
            "manifest entry {} missing sqlite_meta.page_size",
            entry.db_id
        );
    }
}
