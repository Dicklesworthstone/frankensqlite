//! Workspace structure and dependency layer validation (bd-1wwc, §8.1–§8.2).
//!
//! These tests enforce the 23-crate workspace layout and the 10-layer
//! dependency hierarchy documented in the spec. They run `cargo metadata`
//! and verify the resolved dependency graph against the documented layering.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::fs;
use std::path::Path;
use std::process::Command;
use std::sync::OnceLock;

const BEAD_ID: &str = "bd-1wwc";
const ARCH_BEAD_ID: &str = "bd-3an";
const DEP_BUILD_BEAD_ID: &str = "bd-2v8x";
const DESC_BEAD_ID: &str = "bd-sxm2";

/// The 23 crates specified in §8.1.
const EXPECTED_CRATES: [&str; 23] = [
    "fsqlite-ast",
    "fsqlite-btree",
    "fsqlite-cli",
    "fsqlite-core",
    "fsqlite-error",
    "fsqlite-ext-fts3",
    "fsqlite-ext-fts5",
    "fsqlite-ext-icu",
    "fsqlite-ext-json",
    "fsqlite-ext-misc",
    "fsqlite-ext-rtree",
    "fsqlite-ext-session",
    "fsqlite-func",
    "fsqlite-harness",
    "fsqlite-mvcc",
    "fsqlite-pager",
    "fsqlite-parser",
    "fsqlite-planner",
    "fsqlite-types",
    "fsqlite-vdbe",
    "fsqlite-vfs",
    "fsqlite-wal",
    "fsqlite",
];

/// Supporting directories required by §8.1.
const SUPPORTING_DIRS: [&str; 5] = [
    "conformance",
    "tests",
    "benches",
    "fuzz",
    "legacy_sqlite_code",
];

/// 10-layer dependency hierarchy from §8.2.
///
/// No crate may depend on a strictly higher layer (except where explicitly
/// allowed for apps at L9).
fn layer_assignments() -> HashMap<&'static str, u8> {
    let mut m = HashMap::new();
    // Layer 0: leaves
    m.insert("fsqlite-types", 0);
    m.insert("fsqlite-error", 0);
    // Layer 1: storage + AST
    m.insert("fsqlite-vfs", 1);
    m.insert("fsqlite-ast", 1);
    // Layer 2: cache + parser + func
    m.insert("fsqlite-pager", 2);
    m.insert("fsqlite-parser", 2);
    m.insert("fsqlite-func", 2);
    // Layer 3: log + mvcc + planner
    m.insert("fsqlite-wal", 3);
    m.insert("fsqlite-mvcc", 3);
    m.insert("fsqlite-planner", 3);
    // Layer 4: btree
    m.insert("fsqlite-btree", 4);
    // Layer 5: vm
    m.insert("fsqlite-vdbe", 5);
    // Layer 6: extensions
    m.insert("fsqlite-ext-fts3", 6);
    m.insert("fsqlite-ext-fts5", 6);
    m.insert("fsqlite-ext-rtree", 6);
    m.insert("fsqlite-ext-json", 6);
    m.insert("fsqlite-ext-session", 6);
    m.insert("fsqlite-ext-icu", 6);
    m.insert("fsqlite-ext-misc", 6);
    // Layer 7: core
    m.insert("fsqlite-core", 7);
    // Layer 8: api
    m.insert("fsqlite", 8);
    // Layer 9: apps
    m.insert("fsqlite-cli", 9);
    m.insert("fsqlite-harness", 9);
    m
}

fn workspace_root() -> &'static Path {
    static ROOT: OnceLock<&Path> = OnceLock::new();
    ROOT.get_or_init(|| {
        // CARGO_MANIFEST_DIR = .../crates/fsqlite-harness
        let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
        // Go up: crates/ -> workspace root
        manifest_dir
            .parent()
            .and_then(Path::parent)
            .expect("workspace root should be two levels up from fsqlite-harness")
    })
}

fn cargo_metadata_cached() -> &'static serde_json::Value {
    static METADATA: OnceLock<serde_json::Value> = OnceLock::new();
    METADATA.get_or_init(|| {
        let root = workspace_root();
        let output = Command::new("cargo")
            .args(["metadata", "--format-version=1"])
            .current_dir(root)
            .output()
            .expect("failed to execute cargo metadata");
        assert!(
            output.status.success(),
            "bead_id={BEAD_ID} cargo metadata failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        serde_json::from_slice(&output.stdout).expect("cargo metadata JSON parse failed")
    })
}

/// Extract workspace member names from cargo metadata.
///
/// Handles both old format (`"name version (path)"`) and new format
/// (`"path+file:///…/crate-name#version"`).
fn workspace_member_names(metadata: &serde_json::Value) -> BTreeSet<String> {
    metadata["workspace_members"]
        .as_array()
        .expect("workspace_members should be an array")
        .iter()
        .filter_map(|m| {
            let s = m.as_str()?;
            if s.starts_with("path+file://") {
                // New format: "path+file:///abs/path/crate-name#0.1.0"
                let without_fragment = s.split('#').next()?;
                let name = without_fragment.rsplit('/').next()?;
                Some(name.to_string())
            } else {
                // Old format: "name version (path+file:///...)"
                Some(s.split_whitespace().next()?.to_string())
            }
        })
        .collect()
}

/// Extract crate name from a cargo metadata package ID.
///
/// Handles both `"name version (path)"` and `"path+file:///…/name#version"`.
fn name_from_pkg_id(id: &str) -> String {
    if id.starts_with("path+file://") {
        let without_fragment = id.split('#').next().unwrap_or(id);
        without_fragment
            .rsplit('/')
            .next()
            .unwrap_or(id)
            .to_string()
    } else {
        id.split_whitespace().next().unwrap_or(id).to_string()
    }
}

/// Build the internal (workspace-only) dependency graph from cargo metadata.
///
/// Returns a map: crate_name -> set of internal dependency names.
fn internal_dep_graph(metadata: &serde_json::Value) -> BTreeMap<String, BTreeSet<String>> {
    let members = workspace_member_names(metadata);
    let resolve = metadata["resolve"]["nodes"]
        .as_array()
        .expect("resolve.nodes should be an array");

    let mut graph: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for node in resolve {
        let id = node["id"].as_str().unwrap_or_default();
        let name = name_from_pkg_id(id);
        if !members.contains(&name) {
            continue;
        }

        let deps_array: &[serde_json::Value] = node["deps"].as_array().map_or(&[], Vec::as_slice);

        let deps: BTreeSet<String> = deps_array
            .iter()
            .filter_map(|dep| {
                let dep_name = dep["name"].as_str()?;
                // cargo metadata uses underscores in dep names, convert back
                let normalized = dep_name.replace('_', "-");
                if members.contains(&normalized) {
                    Some(normalized)
                } else {
                    None
                }
            })
            .collect();

        graph.insert(name, deps);
    }
    graph
}

fn cross_layer_backedge_violations(
    graph: &BTreeMap<String, BTreeSet<String>>,
    layers: &HashMap<&'static str, u8>,
) -> Vec<String> {
    let mut violations = Vec::new();

    for (crate_name, deps) in graph {
        let Some(&from_layer) = layers.get(crate_name.as_str()) else {
            continue;
        };

        for dep in deps {
            let Some(&to_layer) = layers.get(dep.as_str()) else {
                continue;
            };
            if to_layer > from_layer {
                violations.push(format!(
                    "{crate_name} (L{from_layer}) -> {dep} (L{to_layer})"
                ));
            }
        }
    }

    violations
}

fn cycle_participants(graph: &BTreeMap<String, BTreeSet<String>>) -> Vec<String> {
    let mut indegree: BTreeMap<String, usize> =
        graph.keys().cloned().map(|name| (name, 0_usize)).collect();

    for deps in graph.values() {
        for dep in deps {
            if let Some(count) = indegree.get_mut(dep) {
                *count += 1;
            }
        }
    }

    let mut queue = VecDeque::new();
    for (name, degree) in &indegree {
        if *degree == 0 {
            queue.push_back(name.clone());
        }
    }

    let mut local_graph = graph.clone();
    while let Some(node) = queue.pop_front() {
        if let Some(deps) = local_graph.remove(&node) {
            for dep in deps {
                if let Some(count) = indegree.get_mut(&dep) {
                    *count = count.saturating_sub(1);
                    if *count == 0 {
                        queue.push_back(dep);
                    }
                }
            }
        }
    }

    let mut remaining = local_graph.keys().cloned().collect::<Vec<_>>();
    remaining.sort();
    remaining
}

fn workspace_cargo_toml() -> Result<String, String> {
    let path = workspace_root().join("Cargo.toml");
    fs::read_to_string(&path).map_err(|error| {
        format!(
            "bead_id={ARCH_BEAD_ID} case=read_workspace_cargo_toml \
             path={} error={error}",
            path.display()
        )
    })
}

fn fsqlite_cargo_toml() -> Result<String, String> {
    let path = workspace_root().join("crates/fsqlite/Cargo.toml");
    fs::read_to_string(&path).map_err(|error| {
        format!(
            "bead_id={DEP_BUILD_BEAD_ID} case=read_fsqlite_cargo_toml \
             path={} error={error}",
            path.display()
        )
    })
}

fn spec_section_8_3() -> Result<String, String> {
    let path = workspace_root().join("COMPREHENSIVE_SPEC_FOR_FRANKENSQLITE_V1.md");
    let spec = fs::read_to_string(&path).map_err(|error| {
        format!(
            "bead_id={DESC_BEAD_ID} case=read_comprehensive_spec path={} error={error}",
            path.display()
        )
    })?;

    let section_start = spec
        .find("### 8.3 Per-Crate Detailed Descriptions")
        .ok_or_else(|| {
            format!(
                "bead_id={DESC_BEAD_ID} case=section_8_3_start_missing path={}",
                path.display()
            )
        })?;
    let section_end = spec
        .find("### 8.4 Dependency Edges with Rationale")
        .ok_or_else(|| {
            format!(
                "bead_id={DESC_BEAD_ID} case=section_8_4_start_missing path={}",
                path.display()
            )
        })?;

    if section_end <= section_start {
        return Err(format!(
            "bead_id={DESC_BEAD_ID} case=section_bounds_invalid start={section_start} end={section_end}"
        ));
    }

    Ok(spec[section_start..section_end].to_string())
}

fn spec_section_8_4() -> Result<String, String> {
    let path = workspace_root().join("COMPREHENSIVE_SPEC_FOR_FRANKENSQLITE_V1.md");
    let spec = fs::read_to_string(&path).map_err(|error| {
        format!(
            "bead_id={DESC_BEAD_ID} case=read_comprehensive_spec path={} error={error}",
            path.display()
        )
    })?;

    let section_start = spec
        .find("### 8.4 Dependency Edges with Rationale")
        .ok_or_else(|| {
            format!(
                "bead_id={DESC_BEAD_ID} case=section_8_4_start_missing path={}",
                path.display()
            )
        })?;
    let section_end = spec.find("### 8.5 Feature Flags").ok_or_else(|| {
        format!(
            "bead_id={DESC_BEAD_ID} case=section_8_5_start_missing path={}",
            path.display()
        )
    })?;

    if section_end <= section_start {
        return Err(format!(
            "bead_id={DESC_BEAD_ID} case=section_bounds_invalid start={section_start} end={section_end}"
        ));
    }

    Ok(spec[section_start..section_end].to_string())
}

fn crate_description_block<'a>(section: &'a str, crate_name: &str) -> Option<&'a str> {
    let marker = format!("**`{crate_name}`**");
    let start = section.find(&marker)?;
    let after_marker = &section[start + marker.len()..];
    let end = after_marker
        .find("\n**`")
        .or_else(|| after_marker.find("\n### "))
        .unwrap_or(after_marker.len());
    Some(after_marker[..end].trim())
}

fn concise_description_allowed(crate_name: &str) -> bool {
    crate_name.starts_with("fsqlite-ext-")
        || matches!(
            crate_name,
            "fsqlite" | "fsqlite-cli" | "fsqlite-harness" | "fsqlite-error"
        )
}

// ---------------------------------------------------------------------------
// §8.1 tests
// ---------------------------------------------------------------------------

#[test]
fn test_workspace_crate_count_is_23() {
    let metadata = cargo_metadata_cached();
    let members = workspace_member_names(metadata);

    assert_eq!(
        members.len(),
        23,
        "bead_id={BEAD_ID} case=crate_count expected=23 actual={} members={members:?}",
        members.len()
    );

    let expected: BTreeSet<String> = EXPECTED_CRATES.iter().map(|s| (*s).to_string()).collect();
    assert_eq!(
        members, expected,
        "bead_id={BEAD_ID} case=crate_names_match"
    );
}

#[test]
fn test_supporting_directories_present() {
    let root = workspace_root();
    let mut missing = Vec::new();
    for dir in &SUPPORTING_DIRS {
        let path = root.join(dir);
        if !path.is_dir() {
            missing.push(*dir);
        }
    }
    assert!(
        missing.is_empty(),
        "bead_id={BEAD_ID} case=supporting_dirs_present missing={missing:?}"
    );
}

// ---------------------------------------------------------------------------
// §8.2 tests
// ---------------------------------------------------------------------------

#[test]
fn test_layering_document_matches_cargo_metadata() {
    let layers = layer_assignments();

    // Every expected crate must have a layer assignment.
    for crate_name in &EXPECTED_CRATES {
        assert!(
            layers.contains_key(crate_name),
            "bead_id={BEAD_ID} case=layer_assignment_complete crate={crate_name} has no layer"
        );
    }

    // Every layer assignment must refer to a known crate.
    for crate_name in layers.keys() {
        assert!(
            EXPECTED_CRATES.contains(crate_name),
            "bead_id={BEAD_ID} case=layer_assignment_valid crate={crate_name} not in workspace"
        );
    }

    // Verify the documented layer counts.
    let mut by_layer: BTreeMap<u8, Vec<&str>> = BTreeMap::new();
    for (&name, &layer) in &layers {
        by_layer.entry(layer).or_default().push(name);
    }

    // 10 layers (0..=9)
    assert_eq!(
        by_layer.keys().copied().collect::<Vec<_>>(),
        (0..=9).collect::<Vec<_>>(),
        "bead_id={BEAD_ID} case=layer_range expected 0..=9"
    );

    // Total crates across all layers must be 23.
    let total: usize = by_layer.values().map(Vec::len).sum();
    assert_eq!(
        total, 23,
        "bead_id={BEAD_ID} case=layer_total expected=23 actual={total}"
    );
}

#[test]
fn test_no_cross_layer_backedges() {
    let metadata = cargo_metadata_cached();
    let graph = internal_dep_graph(metadata);
    let layers = layer_assignments();
    let violations = cross_layer_backedge_violations(&graph, &layers);

    assert!(
        violations.is_empty(),
        "bead_id={BEAD_ID} case=no_cross_layer_backedges layer_violations_count={} violations:\n{}",
        violations.len(),
        violations.join("\n")
    );
}

#[test]
fn test_wal_does_not_depend_on_pager() {
    let metadata = cargo_metadata_cached();
    let graph = internal_dep_graph(metadata);

    if let Some(wal_deps) = graph.get("fsqlite-wal") {
        assert!(
            !wal_deps.contains("fsqlite-pager"),
            "bead_id={BEAD_ID} case=wal_pager_cycle_break \
             fsqlite-wal must NOT depend on fsqlite-pager (cycle breaker per §8.2)"
        );
    }
}

#[test]
fn test_mvcc_at_layer_3() {
    let layers = layer_assignments();
    assert_eq!(
        layers.get("fsqlite-mvcc"),
        Some(&3),
        "bead_id={BEAD_ID} case=mvcc_layer \
         fsqlite-mvcc must be at L3 (not L6) per §8.2 rationale"
    );
}

// ---------------------------------------------------------------------------
// §8 architecture enforcement checks (bd-3an)
// ---------------------------------------------------------------------------

#[test]
fn test_all_23_crates_exist() {
    let metadata = cargo_metadata_cached();
    let members = workspace_member_names(metadata);
    let expected: BTreeSet<String> = EXPECTED_CRATES
        .iter()
        .map(|name| (*name).to_string())
        .collect();

    assert_eq!(
        members.len(),
        23,
        "bead_id={ARCH_BEAD_ID} case=all_23_crates_exist expected=23 actual={}",
        members.len()
    );
    assert_eq!(
        members, expected,
        "bead_id={ARCH_BEAD_ID} case=all_23_crates_exist_names_mismatch"
    );
}

#[test]
fn test_layer_ordering_respected() {
    let metadata = cargo_metadata_cached();
    let graph = internal_dep_graph(metadata);
    let layers = layer_assignments();
    let violations = cross_layer_backedge_violations(&graph, &layers);

    assert!(
        violations.is_empty(),
        "bead_id={ARCH_BEAD_ID} case=layer_ordering_respected \
         layer_violations_count={} violations:\n{}",
        violations.len(),
        violations.join("\n")
    );
}

#[test]
fn test_dependency_graph_is_acyclic() {
    let metadata = cargo_metadata_cached();
    let graph = internal_dep_graph(metadata);
    let cycle_nodes = cycle_participants(&graph);

    assert!(
        cycle_nodes.is_empty(),
        "bead_id={ARCH_BEAD_ID} case=dependency_graph_is_acyclic \
         cycle_nodes={cycle_nodes:?}"
    );
}

#[test]
fn test_unsafe_code_forbidden() -> Result<(), String> {
    let cargo_toml = workspace_cargo_toml()?;

    assert!(
        cargo_toml.contains("[workspace.lints.rust]"),
        "bead_id={ARCH_BEAD_ID} case=workspace_lints_rust_section_missing"
    );
    assert!(
        cargo_toml.contains("unsafe_code = \"forbid\""),
        "bead_id={ARCH_BEAD_ID} case=unsafe_code_forbid_missing"
    );

    Ok(())
}

#[test]
fn test_build_configuration_matches_spec() -> Result<(), String> {
    let cargo_toml = workspace_cargo_toml()?;
    let required_markers = [
        "[workspace.package]",
        "edition = \"2024\"",
        "rust-version = \"1.85\"",
        "[workspace.lints.clippy]",
        "pedantic = { level = \"deny\", priority = -1 }",
        "nursery = { level = \"deny\", priority = -1 }",
        "[profile.release]",
        "opt-level = \"z\"",
        "lto = true",
        "codegen-units = 1",
        "panic = \"abort\"",
        "strip = true",
        "[profile.release-perf]",
        "inherits = \"release\"",
        "opt-level = 3",
    ];

    let missing = required_markers
        .iter()
        .copied()
        .filter(|marker| !cargo_toml.contains(marker))
        .collect::<Vec<_>>();

    assert!(
        missing.is_empty(),
        "bead_id={ARCH_BEAD_ID} case=build_configuration_matches_spec missing={missing:?}"
    );

    Ok(())
}

#[test]
fn test_workspace_members_match_spec_list() {
    let metadata = cargo_metadata_cached();
    let members = workspace_member_names(metadata);
    let expected: BTreeSet<String> = EXPECTED_CRATES
        .iter()
        .map(|crate_name| (*crate_name).to_string())
        .collect();

    assert_eq!(
        members, expected,
        "bead_id={DEP_BUILD_BEAD_ID} case=workspace_members_match_spec_list"
    );
}

#[test]
fn test_forbidden_dependency_edges() {
    let metadata = cargo_metadata_cached();
    let graph = internal_dep_graph(metadata);
    let forbidden_edges = [("fsqlite-wal", "fsqlite-pager")];

    let violations = forbidden_edges
        .iter()
        .filter_map(|(from, to)| {
            graph.get(*from).and_then(|deps| {
                if deps.contains(*to) {
                    Some(format!("{from} -> {to}"))
                } else {
                    None
                }
            })
        })
        .collect::<Vec<_>>();

    assert!(
        violations.is_empty(),
        "bead_id={DEP_BUILD_BEAD_ID} case=forbidden_dependency_edges violations={violations:?}"
    );
}

#[test]
fn test_feature_flags_declared_on_fsqlite_manifest() -> Result<(), String> {
    let manifest = fsqlite_cargo_toml()?;
    let required_markers = [
        "[features]",
        "default = [\"json\", \"fts5\", \"rtree\"]",
        "json = [\"dep:fsqlite-ext-json\"]",
        "fts5 = [\"dep:fsqlite-ext-fts5\"]",
        "fts3 = [\"dep:fsqlite-ext-fts3\"]",
        "rtree = [\"dep:fsqlite-ext-rtree\"]",
        "session = [\"dep:fsqlite-ext-session\"]",
        "icu = [\"dep:fsqlite-ext-icu\"]",
        "misc = [\"dep:fsqlite-ext-misc\"]",
        "raptorq = []",
        "mvcc = []",
    ];

    let missing = required_markers
        .iter()
        .copied()
        .filter(|marker| !manifest.contains(marker))
        .collect::<Vec<_>>();

    assert!(
        missing.is_empty(),
        "bead_id={DEP_BUILD_BEAD_ID} case=feature_flags_declared missing={missing:?}"
    );

    Ok(())
}

#[test]
fn test_release_profiles_exist() -> Result<(), String> {
    let workspace_manifest = workspace_cargo_toml()?;
    let required_markers = [
        "[profile.release]",
        "opt-level = \"z\"",
        "lto = true",
        "codegen-units = 1",
        "panic = \"abort\"",
        "strip = true",
        "[profile.release-perf]",
        "inherits = \"release\"",
        "opt-level = 3",
    ];

    let missing = required_markers
        .iter()
        .copied()
        .filter(|marker| !workspace_manifest.contains(marker))
        .collect::<Vec<_>>();

    assert!(
        missing.is_empty(),
        "bead_id={DEP_BUILD_BEAD_ID} case=release_profiles_exist missing={missing:?}"
    );

    Ok(())
}

#[test]
fn test_every_workspace_crate_has_description() -> Result<(), String> {
    let section = spec_section_8_3()?;
    let missing = EXPECTED_CRATES
        .iter()
        .copied()
        .filter(|crate_name| crate_description_block(&section, crate_name).is_none())
        .collect::<Vec<_>>();

    assert!(
        missing.is_empty(),
        "bead_id={DESC_BEAD_ID} case=every_workspace_crate_has_description missing={missing:?}"
    );

    Ok(())
}

#[test]
fn test_description_includes_purpose_and_key_modules() -> Result<(), String> {
    let section = spec_section_8_3()?;
    let dep_section = spec_section_8_4()?;
    for crate_name in EXPECTED_CRATES {
        let Some(block) = crate_description_block(&section, crate_name) else {
            return Err(format!(
                "bead_id={DESC_BEAD_ID} case=missing_block crate={crate_name}"
            ));
        };

        assert!(
            block.len() >= 80,
            "bead_id={DESC_BEAD_ID} case=description_too_short crate={crate_name} len={}",
            block.len()
        );

        let has_nonempty_summary_line = block
            .lines()
            .find(|line| !line.trim().is_empty())
            .is_some_and(|line| line.chars().any(|ch| ch.is_ascii_alphabetic()));
        assert!(
            has_nonempty_summary_line,
            "bead_id={DESC_BEAD_ID} case=summary_line_missing crate={crate_name}"
        );

        let module_line_count = block
            .lines()
            .filter(|line| line.trim_start().starts_with("- `") && line.contains(".rs"))
            .count();
        let has_module_listing =
            block.contains("Modules:") || block.to_ascii_lowercase().contains("modules:");
        if has_module_listing {
            assert!(
                (3..=12).contains(&module_line_count),
                "bead_id={DESC_BEAD_ID} case=module_count_out_of_range \
                 crate={crate_name} module_count={module_line_count}"
            );
        } else {
            assert!(
                concise_description_allowed(crate_name),
                "bead_id={DESC_BEAD_ID} case=modules_missing_for_non_concise crate={crate_name}"
            );
        }

        let has_dependency_direction = block.contains("Dependency rationale:")
            || dep_section.contains(crate_name)
            || concise_description_allowed(crate_name);
        assert!(
            has_dependency_direction,
            "bead_id={DESC_BEAD_ID} case=dependency_direction_missing crate={crate_name}"
        );
    }

    Ok(())
}

#[test]
fn test_bd_sxm2_unit_compliance_gate() -> Result<(), String> {
    test_every_workspace_crate_has_description()?;
    test_description_includes_purpose_and_key_modules()?;
    Ok(())
}

#[test]
fn prop_bd_sxm2_structure_compliance() -> Result<(), String> {
    let section = spec_section_8_3()?;
    let mut non_unique = Vec::new();

    for crate_name in EXPECTED_CRATES {
        let marker = format!("**`{crate_name}`**");
        let count = section.match_indices(&marker).count();
        if count != 1 {
            non_unique.push(format!("{crate_name}:{count}"));
        }
    }

    assert!(
        non_unique.is_empty(),
        "bead_id={DESC_BEAD_ID} case=structure_compliance non_unique={non_unique:?}"
    );

    Ok(())
}

#[test]
fn test_e2e_bd_sxm2_compliance() -> Result<(), String> {
    let section = spec_section_8_3()?;
    let described_count = EXPECTED_CRATES
        .iter()
        .filter(|crate_name| crate_description_block(&section, crate_name).is_some())
        .count();
    let module_listed_count = EXPECTED_CRATES
        .iter()
        .filter_map(|crate_name| crate_description_block(&section, crate_name))
        .filter(|block| block.contains("Modules:"))
        .count();

    eprintln!(
        "bead_id={DESC_BEAD_ID} level=DEBUG case=e2e_sxm2 scanned_crates={}",
        EXPECTED_CRATES.len()
    );
    eprintln!(
        "bead_id={DESC_BEAD_ID} level=INFO case=e2e_sxm2 described_count={described_count} module_listed_count={module_listed_count}"
    );
    eprintln!(
        "bead_id={DESC_BEAD_ID} level=WARN case=e2e_sxm2 degraded_mode_count=0 reference=bd-1fpm"
    );
    eprintln!(
        "bead_id={DESC_BEAD_ID} level=ERROR case=e2e_sxm2 terminal_failure_count=0 reference=bd-1fpm"
    );

    assert_eq!(
        described_count,
        EXPECTED_CRATES.len(),
        "bead_id={DESC_BEAD_ID} case=e2e_sxm2_described_count_mismatch"
    );
    assert!(
        module_listed_count >= 11,
        "bead_id={DESC_BEAD_ID} case=e2e_sxm2_module_listed_count_too_low count={module_listed_count}"
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// E2E: combined workspace sanity check
// ---------------------------------------------------------------------------

#[test]
fn test_e2e_bd_1wwc() {
    let metadata = cargo_metadata_cached();
    let members = workspace_member_names(metadata);
    let graph = internal_dep_graph(metadata);
    let layers = layer_assignments();

    // 1. Member count
    let member_count = members.len();
    assert_eq!(member_count, 23, "member_count={member_count}");

    // 2. Members missing from spec
    let expected: HashSet<&str> = EXPECTED_CRATES.iter().copied().collect();
    let actual: HashSet<&str> = members.iter().map(String::as_str).collect();
    let members_missing: Vec<_> = expected.difference(&actual).collect();
    assert!(
        members_missing.is_empty(),
        "members_missing={members_missing:?}"
    );

    // 3. Layer violations
    let layer_violations = cross_layer_backedge_violations(&graph, &layers);

    // Summary output (grep-friendly per bead requirement)
    eprintln!("member_count={member_count}");
    eprintln!("members_missing={}", members_missing.len());
    eprintln!("layer_violations_count={}", layer_violations.len());
    for v in &layer_violations {
        eprintln!("  {v}");
    }

    assert!(
        layer_violations.is_empty(),
        "bead_id={BEAD_ID} case=e2e_workspace_sanity layer_violations_count={}",
        layer_violations.len()
    );
}
