//! Contract tests for supported_surface_matrix.toml (bd-2yqp6.1.1).
//!
//! Ensures the declared parity surface is machine-readable, explicit about
//! exclusions, and complete enough for downstream planning + CI gating.

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

use serde::Deserialize;

const BEAD_ID: &str = "bd-2yqp6.1.1";

#[derive(Debug, Deserialize)]
struct SurfaceManifest {
    meta: ManifestMeta,
    surface: Vec<SurfaceEntry>,
}

#[derive(Debug, Deserialize)]
struct ManifestMeta {
    schema_version: String,
    bead_id: String,
    track_id: String,
    sqlite_target: String,
    generated_at: String,
    contract_owner: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
enum SupportState {
    Supported,
    Partial,
    Excluded,
}

#[derive(Debug, Deserialize)]
struct SurfaceEntry {
    feature_id: String,
    area: String,
    title: String,
    support_state: SupportState,
    rationale: String,
    owner: String,
    target_evidence: Vec<String>,
    verification_status: String,
}

fn load_manifest() -> SurfaceManifest {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../supported_surface_matrix.toml");
    let content = fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!(
            "failed to read supported_surface_matrix.toml at {}: {e}",
            path.display()
        )
    });
    toml::from_str::<SurfaceManifest>(&content).unwrap_or_else(|e| {
        panic!(
            "failed to parse supported_surface_matrix.toml at {}: {e}",
            path.display()
        )
    })
}

#[test]
fn manifest_meta_is_pinned_to_track_a_contract() {
    let manifest = load_manifest();
    assert_eq!(manifest.meta.schema_version, "1.0.0");
    assert_eq!(manifest.meta.bead_id, BEAD_ID);
    assert_eq!(manifest.meta.track_id, "bd-2yqp6.1");
    assert_eq!(manifest.meta.sqlite_target, "3.52.0");
    assert!(!manifest.meta.generated_at.trim().is_empty());
    assert!(!manifest.meta.contract_owner.trim().is_empty());
}

#[test]
fn surface_entries_have_unique_feature_ids_and_sorted_order() {
    let manifest = load_manifest();
    assert!(
        !manifest.surface.is_empty(),
        "supported surface manifest must contain at least one entry"
    );

    let mut seen = BTreeSet::new();
    let mut previous_ordinal: Option<u32> = None;
    for entry in &manifest.surface {
        assert!(
            seen.insert(entry.feature_id.as_str()),
            "duplicate feature_id: {}",
            entry.feature_id
        );

        let ordinal_text =
            entry.feature_id.rsplit('-').next().unwrap_or_else(|| {
                panic!("feature_id missing numeric suffix: {}", entry.feature_id)
            });
        let ordinal = ordinal_text.parse::<u32>().unwrap_or_else(|_| {
            panic!("invalid numeric suffix in feature_id: {}", entry.feature_id)
        });

        if let Some(prev) = previous_ordinal {
            assert!(
                prev < ordinal,
                "feature entries must be sorted by numeric suffix for deterministic diffs: '{}' !< '{}'",
                prev,
                ordinal
            );
        }
        previous_ordinal = Some(ordinal);
    }
}

#[test]
fn each_entry_has_required_contract_fields() {
    let manifest = load_manifest();

    for entry in &manifest.surface {
        assert!(
            !entry.area.trim().is_empty(),
            "empty area for {}",
            entry.feature_id
        );
        assert!(
            !entry.title.trim().is_empty(),
            "empty title for {}",
            entry.feature_id
        );
        assert!(
            !entry.rationale.trim().is_empty(),
            "empty rationale for {}",
            entry.feature_id
        );
        assert!(
            !entry.owner.trim().is_empty(),
            "empty owner for {}",
            entry.feature_id
        );
        assert!(
            !entry.target_evidence.is_empty(),
            "missing target_evidence for {}",
            entry.feature_id
        );
        for evidence in &entry.target_evidence {
            assert!(
                !evidence.trim().is_empty(),
                "blank evidence reference in {}",
                entry.feature_id
            );
        }
        assert!(
            matches!(
                entry.verification_status.as_str(),
                "planned" | "in_progress" | "verified"
            ),
            "invalid verification_status '{}' for {}",
            entry.verification_status,
            entry.feature_id
        );
    }
}

#[test]
fn exclusions_are_explicit_and_meaningful() {
    let manifest = load_manifest();
    let excluded: Vec<&SurfaceEntry> = manifest
        .surface
        .iter()
        .filter(|entry| entry.support_state == SupportState::Excluded)
        .collect();

    assert!(
        !excluded.is_empty(),
        "manifest must include explicit exclusions"
    );
    for entry in excluded {
        assert!(
            entry.rationale.len() >= 24,
            "excluded feature {} must have a non-trivial rationale",
            entry.feature_id
        );
    }
}

#[test]
fn concurrent_writer_design_is_preserved_in_scope_lock() {
    let manifest = load_manifest();

    let concurrent = manifest
        .surface
        .iter()
        .find(|entry| entry.feature_id == "SURF-TXN-MVCC-CONCURRENT-006")
        .expect("missing concurrent writer contract entry");
    assert_eq!(concurrent.support_state, SupportState::Supported);

    let serialized = manifest
        .surface
        .iter()
        .find(|entry| entry.feature_id == "SURF-TXN-SERIALIZED-WRITER-LOCK-007")
        .expect("missing serialized lock exclusion entry");
    assert_eq!(serialized.support_state, SupportState::Excluded);
}
