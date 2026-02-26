//! Contract tests for runtime_stub_inventory.toml (bd-2yqp6.4.1).
//!
//! Enforces exhaustive runtime NotImplemented/Unsupported/TODO placeholder
//! classification with feature/owner mapping and strict no-drift coverage.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use serde::Deserialize;

const BEAD_ID: &str = "bd-2yqp6.4.1";

#[derive(Debug, Deserialize)]
struct InventoryDocument {
    meta: InventoryMeta,
    runtime_stubs: Vec<RuntimeStub>,
}

#[derive(Debug, Deserialize)]
struct InventoryMeta {
    schema_version: String,
    bead_id: String,
    track_id: String,
    sqlite_target: String,
    generated_at: String,
    contract_owner: String,
    inventory_scope: String,
    source_patterns: Vec<String>,
    parity_critical_severities: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
#[serde(rename_all = "snake_case")]
enum StubKind {
    NotImplemented,
    UnsupportedCodegen,
    TodoPlaceholder,
}

impl StubKind {
    const fn marker(self) -> &'static str {
        match self {
            Self::NotImplemented => "FrankenError::NotImplemented(",
            Self::UnsupportedCodegen => "CodegenError::Unsupported(",
            Self::TodoPlaceholder => "TODO: Apply collation from P4 if present.",
        }
    }
}

#[derive(Debug, Deserialize)]
struct RuntimeStub {
    stub_id: String,
    file: String,
    line: usize,
    kind: StubKind,
    kind_description: String,
    severity: String,
    feature_id: String,
    owner: String,
    closure_strategy: String,
    anchor: String,
}

#[derive(Debug, Deserialize)]
struct SurfaceMatrix {
    surface: Vec<SurfaceEntry>,
}

#[derive(Debug, Deserialize)]
struct SurfaceEntry {
    feature_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct StubKey {
    file: String,
    line: usize,
    kind: StubKind,
}

impl StubKey {
    fn render(&self) -> String {
        format!("{}:{}:{:?}", self.file, self.line, self.kind)
    }
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn read_toml(path: &Path) -> String {
    fs::read_to_string(path).unwrap_or_else(|error| {
        panic!("failed to read {}: {error}", path.display());
    })
}

fn load_inventory() -> InventoryDocument {
    let path = workspace_root().join("runtime_stub_inventory.toml");
    toml::from_str(&read_toml(&path)).unwrap_or_else(|error| {
        panic!("failed to parse {}: {error}", path.display());
    })
}

fn load_surface_ids() -> BTreeSet<String> {
    let path = workspace_root().join("supported_surface_matrix.toml");
    let matrix: SurfaceMatrix = toml::from_str(&read_toml(&path)).unwrap_or_else(|error| {
        panic!("failed to parse {}: {error}", path.display());
    });
    matrix
        .surface
        .into_iter()
        .map(|entry| entry.feature_id)
        .collect()
}

fn detect_runtime_stubs() -> BTreeSet<StubKey> {
    let mut detected = BTreeSet::new();
    let files = [
        "crates/fsqlite-core/src/connection.rs",
        "crates/fsqlite-planner/src/codegen.rs",
        "crates/fsqlite-vdbe/src/codegen.rs",
        "crates/fsqlite-vdbe/src/engine.rs",
    ];

    for rel in files {
        let path = workspace_root().join(rel);
        let content = fs::read_to_string(&path).unwrap_or_else(|error| {
            panic!("failed to read {}: {error}", path.display());
        });
        let lines: Vec<&str> = content.lines().collect();
        let first_cfg_test = lines
            .iter()
            .position(|line| line.contains("#[cfg(test)]"))
            .unwrap_or(lines.len());

        for (index, line) in lines.iter().take(first_cfg_test).enumerate() {
            let line_no = index + 1;
            let kind = if line.contains(StubKind::NotImplemented.marker()) {
                Some(StubKind::NotImplemented)
            } else if line.contains(StubKind::UnsupportedCodegen.marker()) {
                Some(StubKind::UnsupportedCodegen)
            } else if line.contains(StubKind::TodoPlaceholder.marker()) {
                Some(StubKind::TodoPlaceholder)
            } else {
                None
            };

            if let Some(kind) = kind {
                detected.insert(StubKey {
                    file: rel.to_owned(),
                    line: line_no,
                    kind,
                });
            }
        }
    }

    detected
}

fn inventory_keys(doc: &InventoryDocument) -> BTreeSet<StubKey> {
    doc.runtime_stubs
        .iter()
        .map(|stub| StubKey {
            file: stub.file.clone(),
            line: stub.line,
            kind: stub.kind,
        })
        .collect()
}

#[test]
fn inventory_meta_contract_matches_bead() {
    let doc = load_inventory();
    assert_eq!(doc.meta.schema_version, "1.0.0");
    assert_eq!(doc.meta.bead_id, BEAD_ID);
    assert_eq!(doc.meta.track_id, "bd-2yqp6.4");
    assert_eq!(doc.meta.sqlite_target, "3.52.0");
    assert!(!doc.meta.generated_at.trim().is_empty());
    assert!(!doc.meta.contract_owner.trim().is_empty());
    assert!(!doc.meta.inventory_scope.trim().is_empty());
    assert!(!doc.meta.source_patterns.is_empty());
    assert!(!doc.meta.parity_critical_severities.is_empty());
}

#[test]
fn inventory_entries_are_unique_and_well_formed() {
    let doc = load_inventory();
    assert!(
        !doc.runtime_stubs.is_empty(),
        "runtime_stub_inventory.toml must not be empty"
    );

    let surface_ids = load_surface_ids();
    let allowed_severity: BTreeSet<&str> = ["critical", "high", "medium", "low"].into_iter().collect();
    let allowed_strategy: BTreeSet<&str> = ["implement", "explicit_exclusion"].into_iter().collect();

    let mut seen_stub_ids = BTreeSet::new();
    let mut seen_keys = BTreeSet::new();

    for stub in &doc.runtime_stubs {
        assert!(
            seen_stub_ids.insert(stub.stub_id.as_str()),
            "duplicate stub_id: {}",
            stub.stub_id
        );
        let key = format!("{}:{}:{:?}", stub.file, stub.line, stub.kind);
        assert!(seen_keys.insert(key.clone()), "duplicate runtime stub key: {key}");

        assert!(stub.line > 0, "line must be > 0 for {}", stub.stub_id);
        assert!(!stub.kind_description.trim().is_empty(), "missing kind_description for {}", stub.stub_id);
        assert!(
            allowed_severity.contains(stub.severity.as_str()),
            "invalid severity '{}' for {}",
            stub.severity,
            stub.stub_id
        );
        assert!(
            allowed_strategy.contains(stub.closure_strategy.as_str()),
            "invalid closure_strategy '{}' for {}",
            stub.closure_strategy,
            stub.stub_id
        );
        assert!(!stub.owner.trim().is_empty(), "missing owner for {}", stub.stub_id);
        assert!(!stub.anchor.trim().is_empty(), "missing anchor for {}", stub.stub_id);
        assert!(
            surface_ids.contains(&stub.feature_id),
            "unknown feature_id '{}' for {}",
            stub.feature_id,
            stub.stub_id
        );
    }
}

#[test]
fn runtime_stub_inventory_is_exhaustive_for_runtime_scan() {
    let doc = load_inventory();
    let expected = detect_runtime_stubs();
    let actual = inventory_keys(&doc);

    let missing: Vec<String> = expected
        .difference(&actual)
        .map(StubKey::render)
        .collect();
    assert!(
        missing.is_empty(),
        "uncategorized parity-critical stubs detected: {missing:?}"
    );

    let stale: Vec<String> = actual
        .difference(&expected)
        .map(StubKey::render)
        .collect();
    assert!(
        stale.is_empty(),
        "runtime_stub_inventory.toml has stale entries that no longer match runtime scan: {stale:?}"
    );
}

#[test]
fn inventory_file_line_markers_match_current_source() {
    let doc = load_inventory();

    for stub in &doc.runtime_stubs {
        let path = workspace_root().join(&stub.file);
        let content = fs::read_to_string(&path).unwrap_or_else(|error| {
            panic!("failed to read {}: {error}", path.display());
        });
        let lines: Vec<&str> = content.lines().collect();
        let line = lines.get(stub.line.saturating_sub(1)).unwrap_or_else(|| {
            panic!(
                "{} references missing line {} in {}",
                stub.stub_id,
                stub.line,
                stub.file
            )
        });

        assert!(
            line.contains(stub.kind.marker()),
            "{} marker mismatch at {}:{}; expected marker '{}'", 
            stub.stub_id,
            stub.file,
            stub.line,
            stub.kind.marker()
        );
    }
}

#[test]
fn parity_critical_severities_are_fully_classified() {
    let doc = load_inventory();
    let critical_levels: BTreeSet<&str> = doc
        .meta
        .parity_critical_severities
        .iter()
        .map(String::as_str)
        .collect();

    assert!(
        critical_levels.contains("critical") || critical_levels.contains("high"),
        "expected critical/high in meta.parity_critical_severities"
    );

    let uncategorized: Vec<&RuntimeStub> = doc
        .runtime_stubs
        .iter()
        .filter(|stub| critical_levels.contains(stub.severity.as_str()) && stub.feature_id.trim().is_empty())
        .collect();

    assert!(
        uncategorized.is_empty(),
        "parity-critical stubs must have feature mappings"
    );
}
