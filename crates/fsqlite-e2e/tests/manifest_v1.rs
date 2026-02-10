//! Validate that the tracked corpus manifest is present and consistent with
//! `checksums.sha256` and `metadata/*.json`.
//!
//! This test is CI-friendly: it does NOT require the golden `.db` binaries to
//! be present (they are git-ignored). It only reads git-tracked artifacts.

use std::collections::{HashMap, HashSet};
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

#[derive(Debug, Deserialize)]
struct DbProfileLite {
    file_size_bytes: u64,
    page_size: u32,
    #[allow(dead_code)]
    journal_mode: String,
    #[allow(dead_code)]
    user_version: u32,
    #[allow(dead_code)]
    application_id: u32,
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

fn valid_db_id(db_id: &str) -> bool {
    // Pattern in schema: ^[a-z0-9][a-z0-9_\-]{1,63}$
    let bytes = db_id.as_bytes();
    if bytes.len() < 2 || bytes.len() > 64 {
        return false;
    }
    fn is_first(b: u8) -> bool {
        matches!(b, b'a'..=b'z' | b'0'..=b'9')
    }
    fn is_rest(b: u8) -> bool {
        matches!(b, b'a'..=b'z' | b'0'..=b'9' | b'_' | b'-')
    }
    if !is_first(bytes[0]) {
        return false;
    }
    bytes[1..].iter().copied().all(is_rest)
}

fn valid_sha256_hex_lower(s: &str) -> bool {
    if s.len() != 64 {
        return false;
    }
    s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

fn parse_checksums_sha256(path: &Path) -> HashMap<String, String> {
    let content = std::fs::read_to_string(path).expect("failed to read checksums.sha256");
    let mut out = HashMap::new();
    for (i, line) in content.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let parts: Vec<&str> = line.splitn(2, "  ").collect();
        assert_eq!(
            parts.len(),
            2,
            "checksums.sha256 line {} malformed: {line}",
            i + 1
        );
        let sha = parts[0].to_owned();
        let name = parts[1].to_owned();
        assert!(
            valid_sha256_hex_lower(&sha),
            "checksums.sha256 line {} sha is not lowercase 64-hex: {sha}",
            i + 1
        );
        out.insert(name, sha);
    }
    assert!(
        !out.is_empty(),
        "checksums.sha256 must contain at least one entry"
    );
    out
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

    // Validate checksums consistency.
    let checksums_path = corpus_path("sample_sqlite_db_files/checksums.sha256");
    let checksum_map = parse_checksums_sha256(&checksums_path);

    let mut seen_ids: HashSet<&str> = HashSet::new();

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

        let expected_sha = checksum_map.get(&entry.golden_filename).unwrap_or_else(|| {
            panic!(
                "manifest entry {} refers to {}, but it is missing from checksums.sha256",
                entry.db_id, entry.golden_filename
            )
        });
        assert_eq!(
            expected_sha, &entry.sha256_golden,
            "manifest sha mismatch for {} ({})",
            entry.db_id, entry.golden_filename
        );

        // Validate metadata consistency.
        let meta_path = corpus_path(&format!(
            "sample_sqlite_db_files/metadata/{}.json",
            entry.db_id
        ));
        assert!(
            meta_path.exists(),
            "metadata file missing for {}: {}",
            entry.db_id,
            meta_path.display()
        );

        let meta_raw = std::fs::read_to_string(&meta_path).expect("read metadata json");
        let meta: DbProfileLite = serde_json::from_str(&meta_raw).expect("parse metadata json");

        assert_eq!(
            meta.file_size_bytes, entry.size_bytes,
            "size_bytes mismatch for {} (metadata vs manifest)",
            entry.db_id
        );

        // Acceptance requires the manifest to capture page_size.
        let Some(sqlite_meta) = entry.sqlite_meta.as_ref() else {
            panic!("manifest entry {} missing sqlite_meta", entry.db_id);
        };
        let Some(page_size) = sqlite_meta.page_size else {
            panic!(
                "manifest entry {} missing sqlite_meta.page_size",
                entry.db_id
            );
        };
        assert_eq!(
            page_size, meta.page_size,
            "page_size mismatch for {} (metadata vs manifest)",
            entry.db_id
        );
    }
}
