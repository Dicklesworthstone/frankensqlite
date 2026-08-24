//! Contract tests for runtime_stub_inventory.toml (bd-2yqp6.4.1).
//!
//! Enforces exhaustive runtime NotImplemented/Unsupported/TODO placeholder
//! classification with feature/owner mapping and strict no-drift coverage.
//!
//! bd-y7otm / GH#136 item 2: identity is a line-independent **semantic
//! fingerprint** `(file, kind, enclosing_item, normalized_payload)`. `line` is
//! demoted to a location hint that may drift without failing the gate — the
//! exhaustiveness and marker-presence checks match on the fingerprint multiset,
//! so a pure line move never reds the gate and only a real content change (a
//! new/removed marker, a changed diagnostic, or a marker relocated into a
//! different item) requires an inventory edit.

use std::collections::BTreeSet;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde::Deserialize;

const BEAD_ID: &str = "bd-2yqp6.4.1";

#[derive(Debug, Deserialize)]
struct InventoryDocument {
    meta: InventoryMeta,
    runtime_stubs: Vec<RuntimeStub>,
    resolved_runtime_stubs: Vec<ResolvedRuntimeStub>,
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
    // bd-y7otm: next allocatable active ordinal, so additions never reuse a
    // retired id and never force a renumber of existing entries. `#[serde(default)]`
    // only so the pre-migration TOML can be loaded by the regenerator; the gate
    // asserts `next_active_id > 0`, so the emitted contract must carry it.
    #[serde(default)]
    next_active_id: u32,
    // Files scanned for markers; drives the scanner instead of a hardcoded list.
    // `#[serde(default)]` for the same bootstrap reason; the gate asserts non-empty.
    #[serde(default)]
    scanned_files: Vec<String>,
}

/// Fallback scan list used only when bootstrapping the first fingerprint
/// regeneration from a pre-migration inventory whose meta lacks `scanned_files`.
const BOOTSTRAP_SCANNED_FILES: [&str; 4] = [
    "crates/fsqlite-core/src/connection.rs",
    "crates/fsqlite-planner/src/codegen.rs",
    "crates/fsqlite-vdbe/src/codegen.rs",
    "crates/fsqlite-vdbe/src/engine.rs",
];

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

    const fn all() -> [Self; 3] {
        [
            Self::NotImplemented,
            Self::UnsupportedCodegen,
            Self::TodoPlaceholder,
        ]
    }
}

#[derive(Debug, Deserialize)]
struct RuntimeStub {
    stub_id: String,
    file: String,
    /// Location hint only — may drift without failing the gate.
    line: usize,
    kind: StubKind,
    kind_description: String,
    severity: String,
    feature_id: String,
    owner: String,
    closure_strategy: String,
    /// Raw marker line text (human-readable hint).
    anchor: String,
    /// Fingerprint component: nearest enclosing `fn` item. `#[serde(default)]`
    /// only for bootstrap loading; the gate asserts it is non-empty.
    #[serde(default)]
    enclosing_item: String,
    /// Fingerprint component: normalized diagnostic payload. `#[serde(default)]`
    /// only for bootstrap loading; the gate asserts it is non-empty.
    #[serde(default)]
    payload: String,
}

#[derive(Debug, Deserialize)]
struct ResolvedRuntimeStub {
    stub_id: String,
    superseded_stub_id: Option<String>,
    identity_note: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SurfaceMatrix {
    surface: Vec<SurfaceEntry>,
}

#[derive(Debug, Deserialize)]
struct SurfaceEntry {
    feature_id: String,
}

/// Line-independent semantic identity for a runtime-stub marker.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Fingerprint {
    file: String,
    kind: StubKind,
    enclosing_item: String,
    payload: String,
}

impl Fingerprint {
    fn render(&self) -> String {
        format!(
            "{}::{}[{:?}] {:?}",
            self.file, self.enclosing_item, self.kind, self.payload
        )
    }
}

/// One marker discovered by scanning the current source.
#[derive(Debug, Clone)]
struct ScannedMarker {
    file: String,
    line: usize,
    kind: StubKind,
    enclosing_item: String,
    anchor: String,
    payload: String,
}

impl ScannedMarker {
    fn fingerprint(&self) -> Fingerprint {
        Fingerprint {
            file: self.file.clone(),
            kind: self.kind,
            enclosing_item: self.enclosing_item.clone(),
            payload: self.payload.clone(),
        }
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
    let path = workspace_root().join("docs/contracts/runtime_stub_inventory.toml");
    toml::from_str(&read_toml(&path)).unwrap_or_else(|error| {
        panic!("failed to parse {}: {error}", path.display());
    })
}

fn load_surface_ids() -> BTreeSet<String> {
    let path = workspace_root().join("docs/contracts/supported_surface_matrix.toml");
    let matrix: SurfaceMatrix = toml::from_str(&read_toml(&path)).unwrap_or_else(|error| {
        panic!("failed to parse {}: {error}", path.display());
    });
    matrix
        .surface
        .into_iter()
        .map(|entry| entry.feature_id)
        .collect()
}

// ─── Fingerprint scanner ────────────────────────────────────────────────
//
// The scanner and the inventory MUST compute identical fingerprints, so the
// canonical inventory is regenerated by `regenerate_inventory_toml` from this
// exact logic (see the `#[ignore]` test below).

/// Extract the leftmost `fn <ident>` name from a source line, honoring a word
/// boundary before `fn` so `transform`/`fnarg` do not match.
fn extract_fn_name(line: &str) -> Option<String> {
    let bytes = line.as_bytes();
    let mut i = 0;
    while i + 2 <= bytes.len() {
        if &bytes[i..i + 2] == b"fn" {
            let before_ok = i == 0 || !is_ident_byte(bytes[i - 1]);
            let after = bytes.get(i + 2).copied();
            let after_ws = after.is_some_and(|b| b == b' ' || b == b'\t');
            if before_ok && after_ws {
                // skip whitespace, then read identifier
                let mut j = i + 2;
                while j < bytes.len() && (bytes[j] == b' ' || bytes[j] == b'\t') {
                    j += 1;
                }
                let start = j;
                while j < bytes.len() && is_ident_byte(bytes[j]) {
                    j += 1;
                }
                if j > start {
                    return Some(line[start..j].to_owned());
                }
            }
        }
        i += 1;
    }
    None
}

const fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// Nearest enclosing `fn` name at or above `idx`, else `<none>`.
fn enclosing_item(lines: &[&str], idx: usize) -> String {
    for i in (0..=idx).rev() {
        if let Some(name) = extract_fn_name(lines[i]) {
            return name;
        }
    }
    "<none>".to_owned()
}

/// Collapse runs of ASCII whitespace to single spaces and trim.
fn normalize_ws(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// First double-quoted string literal in `text` (handling `\"` escapes).
fn first_string_literal(text: &str) -> Option<String> {
    let bytes = text.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'"' {
            let mut j = i + 1;
            let mut out = String::new();
            while j < bytes.len() {
                if bytes[j] == b'\\' && j + 1 < bytes.len() {
                    out.push(bytes[j] as char);
                    out.push(bytes[j + 1] as char);
                    j += 2;
                    continue;
                }
                if bytes[j] == b'"' {
                    return Some(out);
                }
                out.push(bytes[j] as char);
                j += 1;
            }
            return None;
        }
        i += 1;
    }
    None
}

/// Normalized diagnostic payload for the marker at `idx`: the first string
/// literal in the marker call (across up to 4 physical lines), else the
/// normalized marker line.
fn payload_of(lines: &[&str], idx: usize, marker: &str) -> String {
    let end = (idx + 4).min(lines.len());
    let blob = lines[idx..end]
        .iter()
        .map(|l| l.trim())
        .collect::<Vec<_>>()
        .join(" ");
    let seg = blob.find(marker).map_or(blob.as_str(), |p| &blob[p..]);
    first_string_literal(seg).map_or_else(|| normalize_ws(lines[idx]), |s| normalize_ws(&s))
}

fn classify(line: &str) -> Option<StubKind> {
    StubKind::all().into_iter().find(|k| line.contains(k.marker()))
}

/// Scan the metadata-declared source files (up to the first `#[cfg(test)]`
/// boundary) for runtime-stub markers, computing each marker's fingerprint.
fn scan_markers(scanned_files: &[String]) -> Vec<ScannedMarker> {
    let mut found = Vec::new();
    for rel in scanned_files {
        let path = workspace_root().join(rel);
        let content = fs::read_to_string(&path).unwrap_or_else(|error| {
            panic!("failed to read {}: {error}", path.display());
        });
        let lines: Vec<&str> = content.lines().collect();
        let cut = lines
            .iter()
            .position(|l| l.contains("#[cfg(test)]"))
            .unwrap_or(lines.len());
        for (index, line) in lines.iter().take(cut).enumerate() {
            if let Some(kind) = classify(line) {
                found.push(ScannedMarker {
                    file: rel.clone(),
                    line: index + 1,
                    kind,
                    enclosing_item: enclosing_item(&lines, index),
                    anchor: line.trim().to_owned(),
                    payload: payload_of(&lines, index, kind.marker()),
                });
            }
        }
    }
    found
}

fn inventory_fingerprints(doc: &InventoryDocument) -> Vec<Fingerprint> {
    doc.runtime_stubs
        .iter()
        .map(|stub| Fingerprint {
            file: stub.file.clone(),
            kind: stub.kind,
            enclosing_item: stub.enclosing_item.clone(),
            payload: stub.payload.clone(),
        })
        .collect()
}

fn multiset(items: impl IntoIterator<Item = Fingerprint>) -> BTreeMap<String, u32> {
    let mut counts: BTreeMap<String, u32> = BTreeMap::new();
    for fp in items {
        *counts.entry(fp.render()).or_insert(0) += 1;
    }
    counts
}

// ─── Tests ──────────────────────────────────────────────────────────────

#[test]
fn inventory_meta_contract_matches_bead() {
    let doc = load_inventory();
    assert_eq!(doc.meta.schema_version, "1.1.0");
    assert_eq!(doc.meta.bead_id, BEAD_ID);
    assert_eq!(doc.meta.track_id, "bd-2yqp6.4");
    assert_eq!(doc.meta.sqlite_target, "3.52.0");
    assert!(!doc.meta.generated_at.trim().is_empty());
    assert!(!doc.meta.contract_owner.trim().is_empty());
    assert!(!doc.meta.inventory_scope.trim().is_empty());
    assert!(!doc.meta.source_patterns.is_empty());
    assert!(!doc.meta.parity_critical_severities.is_empty());
    assert!(!doc.meta.scanned_files.is_empty());
    assert!(doc.meta.next_active_id > 0);
}

#[test]
fn inventory_entries_are_unique_and_well_formed() {
    let doc = load_inventory();
    assert!(
        !doc.runtime_stubs.is_empty(),
        "runtime_stub_inventory.toml must not be empty"
    );

    let surface_ids = load_surface_ids();
    let allowed_severity: BTreeSet<&str> =
        ["critical", "high", "medium", "low"].into_iter().collect();
    let allowed_strategy: BTreeSet<&str> =
        ["implement", "explicit_exclusion"].into_iter().collect();

    let mut seen_stub_ids = BTreeSet::new();
    let mut max_active_ordinal = 0u32;

    for stub in &doc.runtime_stubs {
        assert!(
            seen_stub_ids.insert(stub.stub_id.as_str()),
            "duplicate stub_id: {}",
            stub.stub_id
        );
        if let Some(ordinal) = stub
            .stub_id
            .strip_prefix("RSTUB-ACTIVE-")
            .and_then(|n| n.parse::<u32>().ok())
        {
            max_active_ordinal = max_active_ordinal.max(ordinal);
        }

        assert!(stub.line > 0, "line must be > 0 for {}", stub.stub_id);
        assert!(
            !stub.kind_description.trim().is_empty(),
            "missing kind_description for {}",
            stub.stub_id
        );
        assert!(
            !stub.enclosing_item.trim().is_empty(),
            "missing enclosing_item for {}",
            stub.stub_id
        );
        assert!(
            !stub.payload.trim().is_empty(),
            "missing payload for {}",
            stub.stub_id
        );
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
        assert!(
            !stub.owner.trim().is_empty(),
            "missing owner for {}",
            stub.stub_id
        );
        assert!(
            !stub.anchor.trim().is_empty(),
            "missing anchor for {}",
            stub.stub_id
        );
        assert!(
            surface_ids.contains(&stub.feature_id),
            "unknown feature_id '{}' for {}",
            stub.feature_id,
            stub.stub_id
        );
    }

    // The next allocatable id must be strictly beyond every active ordinal so a
    // future addition never collides with an existing entry (bd-y7otm AC5).
    assert!(
        doc.meta.next_active_id > max_active_ordinal,
        "meta.next_active_id ({}) must exceed the highest active ordinal ({max_active_ordinal})",
        doc.meta.next_active_id
    );

    for stub in &doc.resolved_runtime_stubs {
        assert!(
            seen_stub_ids.insert(stub.stub_id.as_str()),
            "duplicate stub_id across active and resolved entries: {}",
            stub.stub_id
        );
        match (&stub.superseded_stub_id, &stub.identity_note) {
            (Some(superseded), Some(note)) => {
                assert_ne!(
                    superseded, &stub.stub_id,
                    "renumbered stub {} must supersede a different ID",
                    stub.stub_id
                );
                assert!(
                    !superseded.trim().is_empty() && !note.trim().is_empty(),
                    "renumbered stub {} requires non-empty identity provenance",
                    stub.stub_id
                );
            }
            (None, None) => {}
            _ => panic!(
                "renumbered stub {} must record both superseded_stub_id and identity_note",
                stub.stub_id
            ),
        }
    }
}

#[test]
fn runtime_stub_inventory_is_exhaustive_for_runtime_scan() {
    let doc = load_inventory();
    let scanned = multiset(scan_markers(&doc.meta.scanned_files).iter().map(ScannedMarker::fingerprint));
    let inventory = multiset(inventory_fingerprints(&doc));

    // Fingerprints in the scan but missing (or under-counted) in the inventory.
    let mut missing = Vec::new();
    for (fp, scanned_count) in &scanned {
        let inv_count = inventory.get(fp).copied().unwrap_or(0);
        if inv_count < *scanned_count {
            missing.push(format!("{fp} (scanned {scanned_count}, inventory {inv_count})"));
        }
    }
    assert!(
        missing.is_empty(),
        "uncategorized parity-critical stubs detected (line-independent): {missing:?}"
    );

    // Fingerprints in the inventory but absent (or over-counted) in the scan.
    let mut stale = Vec::new();
    for (fp, inv_count) in &inventory {
        let scanned_count = scanned.get(fp).copied().unwrap_or(0);
        if *inv_count > scanned_count {
            stale.push(format!("{fp} (inventory {inv_count}, scanned {scanned_count})"));
        }
    }
    assert!(
        stale.is_empty(),
        "runtime_stub_inventory.toml has stale entries that no longer match runtime scan: {stale:?}"
    );
}

#[test]
fn inventory_fingerprints_match_current_source() {
    // Every inventory entry must correspond to a real marker in the current
    // source *by fingerprint* — line is a hint that may drift. Ambiguity (an
    // inventory fingerprint appearing more times than the scan) is surfaced by
    // the exhaustiveness test above; here we assert presence and report drift.
    let doc = load_inventory();
    let scanned = scan_markers(&doc.meta.scanned_files);
    let scanned_set = multiset(scanned.iter().map(ScannedMarker::fingerprint));

    for stub in &doc.runtime_stubs {
        let fp = Fingerprint {
            file: stub.file.clone(),
            kind: stub.kind,
            enclosing_item: stub.enclosing_item.clone(),
            payload: stub.payload.clone(),
        };
        assert!(
            scanned_set.contains_key(&fp.render()),
            "{} has no matching marker in current source (fingerprint {}); the marker was removed or its enclosing item/payload changed",
            stub.stub_id,
            fp.render()
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
        .filter(|stub| {
            critical_levels.contains(stub.severity.as_str()) && stub.feature_id.trim().is_empty()
        })
        .collect();

    assert!(
        uncategorized.is_empty(),
        "parity-critical stubs must have feature mappings"
    );
}

#[test]
fn canonical_and_root_inventories_are_byte_identical() {
    let root = workspace_root();
    let canonical = read_toml(&root.join("docs/contracts/runtime_stub_inventory.toml"));
    let mirror = read_toml(&root.join("runtime_stub_inventory.toml"));
    if canonical != mirror {
        let divergence = canonical
            .lines()
            .zip(mirror.lines())
            .enumerate()
            .find(|(_, (left, right))| left != right)
            .map_or_else(
                || {
                    format!(
                        "line counts differ: canonical={}, mirror={}",
                        canonical.lines().count(),
                        mirror.lines().count()
                    )
                },
                |(index, (left, right))| {
                    format!(
                        "first divergence at line {}: canonical={left:?} mirror={right:?}",
                        index + 1
                    )
                },
            );
        panic!("runtime-stub inventory mirrors drifted; {divergence}");
    }
}

// ─── Regression coverage for the fingerprint identity (bd-y7otm) ─────────

#[test]
fn fingerprint_is_stable_under_pure_line_drift() {
    // The same marker inside the same fn with the same payload yields the same
    // fingerprint regardless of its line number.
    let src_a = vec![
        "fn emit_thing() {",
        "    return Err(CodegenError::Unsupported(\"widget lowering\".to_owned()));",
        "}",
    ];
    let src_b = vec![
        "// a newly inserted comment",
        "",
        "fn unrelated() {}",
        "",
        "fn emit_thing() {",
        "    // another inserted line",
        "    return Err(CodegenError::Unsupported(\"widget lowering\".to_owned()));",
        "}",
    ];
    let idx_a = 1;
    let idx_b = 6;
    let marker = StubKind::UnsupportedCodegen.marker();
    assert_eq!(enclosing_item(&src_a, idx_a), "emit_thing");
    assert_eq!(enclosing_item(&src_b, idx_b), "emit_thing");
    assert_eq!(payload_of(&src_a, idx_a, marker), "widget lowering");
    assert_eq!(payload_of(&src_b, idx_b, marker), "widget lowering");
}

#[test]
fn fingerprint_detects_payload_change() {
    let marker = StubKind::UnsupportedCodegen.marker();
    let before = vec![
        "fn emit_thing() {",
        "    return Err(CodegenError::Unsupported(\"old message\".to_owned()));",
        "}",
    ];
    let after = vec![
        "fn emit_thing() {",
        "    return Err(CodegenError::Unsupported(\"new message\".to_owned()));",
        "}",
    ];
    assert_ne!(
        payload_of(&before, 1, marker),
        payload_of(&after, 1, marker),
        "a changed diagnostic payload must change the fingerprint"
    );
}

#[test]
fn enclosing_item_disambiguates_identical_anchor_lines() {
    // Two identical `return Err(CodegenError::Unsupported(` anchors in different
    // functions with different payloads are distinct fingerprints.
    let src = vec![
        "fn lower_join() {",
        "    return Err(CodegenError::Unsupported(",
        "        \"paren join not supported\".to_owned()));",
        "}",
        "fn lower_subquery() {",
        "    return Err(CodegenError::Unsupported(",
        "        \"subquery source not supported\".to_owned()));",
        "}",
    ];
    let marker = StubKind::UnsupportedCodegen.marker();
    assert_eq!(enclosing_item(&src, 1), "lower_join");
    assert_eq!(enclosing_item(&src, 5), "lower_subquery");
    assert_eq!(payload_of(&src, 1, marker), "paren join not supported");
    assert_eq!(payload_of(&src, 5, marker), "subquery source not supported");
}

#[test]
fn payload_multiline_marker_is_captured() {
    // The string literal on a continuation line is still captured.
    let marker = StubKind::UnsupportedCodegen.marker();
    let src = vec![
        "fn emit() {",
        "    return Err(CodegenError::Unsupported(format!(",
        "        \"table {} has {} columns\",",
        "        name, count,",
        "    )));",
        "}",
    ];
    assert_eq!(payload_of(&src, 1, marker), "table {} has {} columns");
}

// ─── Canonical regeneration (bd-y7otm) ───────────────────────────────────
//
// `cargo test -p fsqlite-harness --test bd_2yqp6_4_1_runtime_stub_inventory \
//    -- --ignored regenerate_inventory_toml --nocapture`
// prints the canonical TOML from the current scan; the operator writes it to
// docs/contracts/runtime_stub_inventory.toml and the root mirror. Because the
// gate tests use the same scanner, the regenerated file is green by
// construction. `line` is emitted as a hint; identity is the fingerprint.

fn toml_escape(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

#[test]
#[ignore = "operator-run regenerator; prints the canonical inventory TOML"]
fn regenerate_inventory_toml() {
    // Preserve the existing meta + resolved history; re-baseline the active list
    // from the current scan (uniform metadata, fingerprint identity).
    let existing = load_inventory();
    let scan_files: Vec<String> = if existing.meta.scanned_files.is_empty() {
        BOOTSTRAP_SCANNED_FILES
            .iter()
            .map(|s| (*s).to_owned())
            .collect()
    } else {
        existing.meta.scanned_files.clone()
    };
    let scanned = scan_markers(&scan_files);

    let mut out = String::new();
    out.push_str("[meta]\n");
    out.push_str("schema_version = \"1.1.0\"\n");
    out.push_str(&format!("bead_id = \"{}\"\n", existing.meta.bead_id));
    out.push_str(&format!("track_id = \"{}\"\n", existing.meta.track_id));
    out.push_str(&format!("sqlite_target = \"{}\"\n", existing.meta.sqlite_target));
    out.push_str(&format!("generated_at = \"{}\"\n", existing.meta.generated_at));
    out.push_str(&format!("contract_owner = \"{}\"\n", existing.meta.contract_owner));
    out.push_str(&format!(
        "inventory_scope = \"{}\"\n",
        existing.meta.inventory_scope
    ));
    out.push_str(&format!(
        "identity_model = \"fingerprint (file, kind, enclosing_item, normalized_payload); line is a hint (bd-y7otm/GH#136 item 2)\"\n"
    ));
    let source_patterns = existing
        .meta
        .source_patterns
        .iter()
        .map(|p| format!("\"{}\"", toml_escape(p)))
        .collect::<Vec<_>>()
        .join(", ");
    out.push_str(&format!("source_patterns = [{source_patterns}]\n"));
    let parity = existing
        .meta
        .parity_critical_severities
        .iter()
        .map(|p| format!("\"{}\"", toml_escape(p)))
        .collect::<Vec<_>>()
        .join(", ");
    out.push_str(&format!("parity_critical_severities = [{parity}]\n"));
    let files = scan_files
        .iter()
        .map(|p| format!("\"{}\"", toml_escape(p)))
        .collect::<Vec<_>>()
        .join(", ");
    out.push_str(&format!("scanned_files = [{files}]\n"));
    out.push_str(&format!(
        "next_active_id = {}\n",
        scanned.len() + 1
    ));

    // Active entries in source order; ordinals assigned 1..=N.
    for (index, marker) in scanned.iter().enumerate() {
        out.push_str("\n[[runtime_stubs]]\n");
        out.push_str(&format!("stub_id = \"RSTUB-ACTIVE-{:04}\"\n", index + 1));
        out.push_str(&format!("file = \"{}\"\n", toml_escape(&marker.file)));
        out.push_str(&format!("line = {}\n", marker.line));
        out.push_str(&format!("kind = \"{}\"\n", kind_str(marker.kind)));
        out.push_str("kind_description = \"CodegenError::Unsupported compiler fallback\"\n");
        out.push_str("severity = \"critical\"\n");
        out.push_str("feature_id = \"SURF-SQL-CORE-001\"\n");
        out.push_str("owner = \"track-d-engine-runtime\"\n");
        out.push_str("closure_strategy = \"implement\"\n");
        out.push_str(&format!("anchor = \"{}\"\n", toml_escape(&marker.anchor)));
        out.push_str(&format!(
            "enclosing_item = \"{}\"\n",
            toml_escape(&marker.enclosing_item)
        ));
        out.push_str(&format!("payload = \"{}\"\n", toml_escape(&marker.payload)));
    }

    // Preserve resolved history verbatim.
    for resolved in &existing.resolved_runtime_stubs {
        out.push_str("\n[[resolved_runtime_stubs]]\n");
        out.push_str(&format!("stub_id = \"{}\"\n", toml_escape(&resolved.stub_id)));
        if let (Some(superseded), Some(note)) =
            (&resolved.superseded_stub_id, &resolved.identity_note)
        {
            out.push_str(&format!(
                "superseded_stub_id = \"{}\"\n",
                toml_escape(superseded)
            ));
            out.push_str(&format!("identity_note = \"{}\"\n", toml_escape(note)));
        }
    }

    println!("=====BEGIN_CANONICAL_INVENTORY_TOML=====");
    print!("{out}");
    println!("=====END_CANONICAL_INVENTORY_TOML=====");
}

const fn kind_str(kind: StubKind) -> &'static str {
    match kind {
        StubKind::NotImplemented => "not_implemented",
        StubKind::UnsupportedCodegen => "unsupported_codegen",
        StubKind::TodoPlaceholder => "todo_placeholder",
    }
}
