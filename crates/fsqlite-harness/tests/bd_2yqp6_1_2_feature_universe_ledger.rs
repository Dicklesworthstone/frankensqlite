//! Contract tests for feature_universe_ledger.toml (bd-2yqp6.1.2).
//!
//! Enforces machine-readable canonical ledger integrity and lint-style failure
//! on missing test/evidence links.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

use serde::Deserialize;

const BEAD_ID: &str = "bd-2yqp6.1.2";

#[derive(Debug, Deserialize)]
struct LedgerDocument {
    meta: LedgerMeta,
    features: Vec<LedgerFeature>,
}

#[derive(Debug, Deserialize)]
struct LedgerMeta {
    schema_version: String,
    bead_id: String,
    track_id: String,
    sqlite_target: String,
    generated_at: String,
    contract_owner: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
enum LifecycleState {
    Declared,
    Implemented,
    Tested,
    DifferentiallyVerified,
}

#[derive(Debug, Deserialize)]
struct LedgerFeature {
    feature_id: String,
    surface_id: String,
    component: String,
    feature_name: String,
    lifecycle_state: LifecycleState,
    owner: String,
    test_links: Vec<String>,
    evidence_links: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct SurfaceMatrix {
    surface: Vec<SurfaceEntry>,
}

#[derive(Debug, Deserialize)]
struct SurfaceEntry {
    feature_id: String,
}

fn read_toml(path: &Path) -> String {
    fs::read_to_string(path).unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()))
}

fn load_ledger() -> LedgerDocument {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../feature_universe_ledger.toml");
    toml::from_str(&read_toml(&path))
        .unwrap_or_else(|e| panic!("failed to parse {}: {e}", path.display()))
}

fn load_surface_ids() -> BTreeSet<String> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../supported_surface_matrix.toml");
    let matrix: SurfaceMatrix = toml::from_str(&read_toml(&path))
        .unwrap_or_else(|e| panic!("failed to parse {}: {e}", path.display()));
    matrix
        .surface
        .into_iter()
        .map(|entry| entry.feature_id)
        .collect()
}

#[test]
fn ledger_meta_matches_contract() {
    let doc = load_ledger();
    assert_eq!(doc.meta.schema_version, "1.0.0");
    assert_eq!(doc.meta.bead_id, BEAD_ID);
    assert_eq!(doc.meta.track_id, "bd-2yqp6.1");
    assert_eq!(doc.meta.sqlite_target, "3.52.0");
    assert!(!doc.meta.generated_at.trim().is_empty());
    assert!(!doc.meta.contract_owner.trim().is_empty());
}

#[test]
fn feature_ids_are_unique_and_monotonic() {
    let doc = load_ledger();
    assert!(!doc.features.is_empty(), "ledger must not be empty");

    let mut seen = BTreeSet::new();
    let mut prev: Option<u32> = None;
    for feature in &doc.features {
        assert!(
            seen.insert(feature.feature_id.as_str()),
            "duplicate feature_id: {}",
            feature.feature_id
        );

        let suffix = feature
            .feature_id
            .strip_prefix("LEDGER-")
            .unwrap_or_else(|| panic!("invalid feature id format: {}", feature.feature_id));
        let n = suffix
            .parse::<u32>()
            .unwrap_or_else(|_| panic!("invalid feature id suffix: {}", feature.feature_id));
        if let Some(prev_n) = prev {
            assert!(
                prev_n < n,
                "feature ids must be strictly increasing: {} then {}",
                prev_n,
                n
            );
        }
        prev = Some(n);
    }
}

#[test]
fn every_feature_has_required_links_and_metadata() {
    let doc = load_ledger();
    let allowed_components = ["parser", "planner", "vdbe", "core", "extension"];

    for feature in &doc.features {
        assert!(
            allowed_components.contains(&feature.component.as_str()),
            "invalid component '{}' for {}",
            feature.component,
            feature.feature_id
        );
        assert!(
            !feature.feature_name.trim().is_empty(),
            "missing feature_name for {}",
            feature.feature_id
        );
        assert!(
            !feature.owner.trim().is_empty(),
            "missing owner for {}",
            feature.feature_id
        );
        assert!(
            !feature.test_links.is_empty(),
            "missing test_links for {}",
            feature.feature_id
        );
        assert!(
            !feature.evidence_links.is_empty(),
            "missing evidence_links for {}",
            feature.feature_id
        );
        for link in &feature.test_links {
            assert!(
                !link.trim().is_empty(),
                "blank test link for {}",
                feature.feature_id
            );
        }
        for link in &feature.evidence_links {
            assert!(
                !link.trim().is_empty(),
                "blank evidence link for {}",
                feature.feature_id
            );
        }
    }
}

#[test]
fn ledger_references_valid_surface_ids() {
    let doc = load_ledger();
    let surface_ids = load_surface_ids();

    for feature in &doc.features {
        assert!(
            surface_ids.contains(&feature.surface_id),
            "unknown surface_id '{}' referenced by {}",
            feature.surface_id,
            feature.feature_id
        );
    }
}

#[test]
fn ledger_is_queryable_by_component_and_lifecycle() {
    let doc = load_ledger();
    let mut by_component: BTreeMap<&str, usize> = BTreeMap::new();
    let mut by_lifecycle: BTreeMap<&'static str, usize> = BTreeMap::new();

    for feature in &doc.features {
        *by_component.entry(feature.component.as_str()).or_insert(0) += 1;
        let lifecycle_name = match feature.lifecycle_state {
            LifecycleState::Declared => "declared",
            LifecycleState::Implemented => "implemented",
            LifecycleState::Tested => "tested",
            LifecycleState::DifferentiallyVerified => "differentially_verified",
        };
        *by_lifecycle.entry(lifecycle_name).or_insert(0) += 1;
    }

    for component in ["parser", "planner", "vdbe", "core", "extension"] {
        assert!(
            by_component.get(component).copied().unwrap_or(0) > 0,
            "component '{}' has no ledger entries",
            component
        );
    }
    assert!(
        by_lifecycle.get("declared").copied().unwrap_or(0) > 0,
        "expected declared-stage entries"
    );
    assert!(
        by_lifecycle
            .get("differentially_verified")
            .copied()
            .unwrap_or(0)
            > 0,
        "expected differential verification entries"
    );
}
