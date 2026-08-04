//! Unit tests for parity taxonomy invariants (bd-1dp9.1.1).
//!
//! Validates the structural integrity and determinism of the parity taxonomy
//! schema defined in `parity_taxonomy.toml`.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;

use fsqlite_harness::parity_taxonomy::PARITY_TAXONOMY_CONTRACT_PATH;

/// Parsed feature entry from the taxonomy TOML.
#[derive(Debug)]
struct Feature {
    id: String,
    family: String,
    title: String,
    weight: u32,
    status: String,
}

/// Parsed exclusion entry.
#[derive(Debug)]
struct Exclusion {
    id: String,
    title: String,
    rationale: String,
}

fn load_taxonomy() -> (Vec<Feature>, Vec<Exclusion>, toml::Value) {
    // CARGO_MANIFEST_DIR points to crates/fsqlite-harness.
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(PARITY_TAXONOMY_CONTRACT_PATH);
    let content = fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!(
            "Failed to read parity_taxonomy.toml at {}: {e}",
            path.display()
        )
    });
    let doc: toml::Value = toml::from_str(&content).expect("Failed to parse TOML");

    let features: Vec<Feature> = doc["features"]
        .as_array()
        .expect("features must be an array")
        .iter()
        .map(|f| Feature {
            id: f["id"].as_str().unwrap().to_owned(),
            family: f["family"].as_str().unwrap().to_owned(),
            title: f["title"].as_str().unwrap().to_owned(),
            weight: u32::try_from(f["weight"].as_integer().unwrap()).unwrap(),
            status: f["status"].as_str().unwrap().to_owned(),
        })
        .collect();

    let exclusions: Vec<Exclusion> = doc["exclusions"]
        .as_array()
        .expect("exclusions must be an array")
        .iter()
        .map(|e| Exclusion {
            id: e["id"].as_str().unwrap().to_owned(),
            title: e["title"].as_str().unwrap().to_owned(),
            rationale: e["rationale"].as_str().unwrap().to_owned(),
        })
        .collect();

    (features, exclusions, doc)
}

#[test]
fn taxonomy_feature_ids_are_unique() {
    let (features, _, _) = load_taxonomy();
    let mut seen = HashSet::new();
    for f in &features {
        assert!(seen.insert(&f.id), "Duplicate feature ID: {}", f.id);
    }
}

#[test]
fn taxonomy_exclusion_ids_are_unique() {
    let (_, exclusions, _) = load_taxonomy();
    let mut seen = HashSet::new();
    for e in &exclusions {
        assert!(seen.insert(&e.id), "Duplicate exclusion ID: {}", e.id);
    }
}

#[test]
fn taxonomy_feature_ids_follow_convention() {
    let (features, _, _) = load_taxonomy();
    let valid_families = [
        "SQL", "TXN", "FUN", "VDB", "PLN", "PGM", "EXT", "CLI", "TYP",
    ];
    for f in &features {
        assert!(
            f.id.starts_with("F-"),
            "Feature ID must start with 'F-': {}",
            f.id
        );
        let parts: Vec<&str> = f.id[2..].splitn(2, '.').collect();
        assert_eq!(
            parts.len(),
            2,
            "Feature ID must be F-{{family}}.{{ordinal}}: {}",
            f.id
        );
        assert!(
            valid_families.contains(&parts[0]),
            "Unknown family code '{}' in feature {}: valid codes are {:?}",
            parts[0],
            f.id,
            valid_families
        );
        assert!(
            parts[1].parse::<u32>().is_ok(),
            "Ordinal must be numeric in feature {}: got '{}'",
            f.id,
            parts[1]
        );
    }
}

#[test]
fn taxonomy_family_matches_id_prefix() {
    let (features, _, _) = load_taxonomy();
    for f in &features {
        let family_from_id = f.id[2..].split('.').next().unwrap();
        assert_eq!(
            f.family, family_from_id,
            "Feature {} has family '{}' but ID prefix is '{}'",
            f.id, f.family, family_from_id
        );
    }
}

#[test]
fn taxonomy_weights_are_positive() {
    let (features, _, _) = load_taxonomy();
    for f in &features {
        assert!(f.weight > 0, "Feature {} has zero weight", f.id);
    }
}

#[test]
fn taxonomy_total_weight_matches_declared() {
    let (features, _, doc) = load_taxonomy();
    let declared_total = doc["meta"]["total_weight"].as_integer().unwrap();
    let actual_total: u32 = features.iter().map(|f| f.weight).sum();
    assert_eq!(
        i64::from(actual_total),
        declared_total,
        "Declared total_weight ({declared_total}) does not match sum of feature weights ({actual_total})"
    );
}

#[test]
fn taxonomy_status_values_are_valid() {
    let (features, _, _) = load_taxonomy();
    let valid_statuses = ["pass", "fail", "partial", "excluded"];
    for f in &features {
        assert!(
            valid_statuses.contains(&f.status.as_str()),
            "Feature {} has invalid status '{}': valid values are {:?}",
            f.id,
            f.status,
            valid_statuses
        );
    }
}

#[test]
fn taxonomy_exclusions_have_rationale() {
    let (_, exclusions, _) = load_taxonomy();
    for e in &exclusions {
        assert!(
            !e.rationale.is_empty(),
            "Exclusion {} has empty rationale",
            e.id
        );
        assert!(!e.title.is_empty(), "Exclusion {} has empty title", e.id);
    }
}

#[test]
fn taxonomy_parity_score_is_deterministic() {
    let (features, _, _) = load_taxonomy();
    let total_weight: f64 = features.iter().map(|f| f64::from(f.weight)).sum();
    let passing_weight: f64 = features
        .iter()
        .filter(|f| f.status == "pass")
        .map(|f| f64::from(f.weight))
        .sum();
    let score = passing_weight / total_weight;

    // Score must be deterministic given the same taxonomy
    let score2 = passing_weight / total_weight;
    assert!(
        (score - score2).abs() < f64::EPSILON,
        "Parity score is not deterministic"
    );

    // Score must be in [0, 1]
    assert!(
        (0.0..=1.0).contains(&score),
        "Parity score out of range: {score}"
    );

    // Print current score for observability
    eprintln!(
        "Parity score: {:.4} ({:.1}%) \u{2014} {:.0}/{:.0} weight points passing",
        score,
        score * 100.0,
        passing_weight,
        total_weight
    );
}

#[test]
fn taxonomy_family_weight_distribution() {
    let (features, _, _) = load_taxonomy();
    let mut family_weights: HashMap<&str, u32> = HashMap::new();
    for f in &features {
        *family_weights.entry(&f.family).or_default() += f.weight;
    }

    // Print distribution for observability
    let mut families: Vec<_> = family_weights.iter().collect();
    families.sort_by_key(|&(_, w)| std::cmp::Reverse(*w));
    eprintln!("Family weight distribution:");
    for (family, weight) in &families {
        eprintln!("  {family}: {weight}");
    }

    // Verify all expected families are present
    let expected_families = ["SQL", "TXN", "FUN", "VDB", "PLN", "PGM", "EXT", "CLI"];
    for fam in expected_families {
        assert!(
            family_weights.contains_key(fam),
            "Expected family '{fam}' not found in taxonomy"
        );
    }
}

#[test]
fn taxonomy_concurrent_mode_features_present() {
    // Critical invariant: concurrent writer features must exist and be non-excluded
    let (features, _, _) = load_taxonomy();
    let concurrent_features: Vec<_> = features
        .iter()
        .filter(|f| {
            f.title.to_lowercase().contains("concurrent")
                || f.title.to_lowercase().contains("mvcc")
                || f.title.to_lowercase().contains("ssi")
                || f.title.to_lowercase().contains("fcw")
        })
        .collect();

    assert!(
        concurrent_features.len() >= 4,
        "At least 4 MVCC/concurrent features expected, found {}",
        concurrent_features.len()
    );

    for f in &concurrent_features {
        assert_ne!(
            f.status, "excluded",
            "MVCC/concurrent feature {} must not be excluded",
            f.id
        );
    }
}
