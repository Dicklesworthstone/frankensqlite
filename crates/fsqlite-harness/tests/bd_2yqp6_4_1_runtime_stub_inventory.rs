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

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use proc_macro2::{Span, TokenStream, TokenTree};
use serde::Deserialize;
use syn::spanned::Spanned;
use syn::visit::{self, Visit};
use syn::{Attribute, Expr, ImplItem, Item, LitStr, TraitItem};

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

    fn from_source_pattern(pattern: &str) -> Option<Self> {
        Self::all()
            .into_iter()
            .find(|kind| kind.marker() == pattern)
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

/// Collapse runs of ASCII whitespace to single spaces and trim.
fn normalize_ws(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn declared_stub_kinds(source_patterns: &[String]) -> BTreeSet<StubKind> {
    let mut kinds = BTreeSet::new();
    for pattern in source_patterns {
        let kind = StubKind::from_source_pattern(pattern).unwrap_or_else(|| {
            panic!("unsupported runtime-stub source pattern in metadata: {pattern:?}")
        });
        assert!(
            kinds.insert(kind),
            "duplicate runtime-stub source pattern in metadata: {pattern:?}"
        );
    }
    assert!(
        !kinds.is_empty(),
        "runtime-stub source-pattern metadata must not be empty"
    );
    kinds
}

fn is_cfg_test(attrs: &[Attribute]) -> bool {
    attrs.iter().any(|attr| {
        attr.path().is_ident("cfg")
            && attr
                .parse_args::<syn::Ident>()
                .is_ok_and(|ident| ident == "test")
    })
}

fn item_attrs(item: &Item) -> Option<&[Attribute]> {
    match item {
        Item::Const(item) => Some(&item.attrs),
        Item::Enum(item) => Some(&item.attrs),
        Item::ExternCrate(item) => Some(&item.attrs),
        Item::Fn(item) => Some(&item.attrs),
        Item::ForeignMod(item) => Some(&item.attrs),
        Item::Impl(item) => Some(&item.attrs),
        Item::Macro(item) => Some(&item.attrs),
        Item::Mod(item) => Some(&item.attrs),
        Item::Static(item) => Some(&item.attrs),
        Item::Struct(item) => Some(&item.attrs),
        Item::Trait(item) => Some(&item.attrs),
        Item::TraitAlias(item) => Some(&item.attrs),
        Item::Type(item) => Some(&item.attrs),
        Item::Union(item) => Some(&item.attrs),
        Item::Use(item) => Some(&item.attrs),
        Item::Verbatim(_) => None,
        _ => None,
    }
}

fn impl_item_attrs(item: &ImplItem) -> Option<&[Attribute]> {
    match item {
        ImplItem::Const(item) => Some(&item.attrs),
        ImplItem::Fn(item) => Some(&item.attrs),
        ImplItem::Type(item) => Some(&item.attrs),
        ImplItem::Macro(item) => Some(&item.attrs),
        ImplItem::Verbatim(_) => None,
        _ => None,
    }
}

fn trait_item_attrs(item: &TraitItem) -> Option<&[Attribute]> {
    match item {
        TraitItem::Const(item) => Some(&item.attrs),
        TraitItem::Fn(item) => Some(&item.attrs),
        TraitItem::Type(item) => Some(&item.attrs),
        TraitItem::Macro(item) => Some(&item.attrs),
        TraitItem::Verbatim(_) => None,
        _ => None,
    }
}

fn call_kind(expr: &Expr) -> Option<StubKind> {
    let Expr::Path(path) = expr else {
        return None;
    };
    let mut segments = path.path.segments.iter().rev();
    let variant = segments.next()?.ident.to_string();
    let ty = segments.next()?.ident.to_string();
    match (ty.as_str(), variant.as_str()) {
        ("FrankenError", "NotImplemented") => Some(StubKind::NotImplemented),
        ("CodegenError", "Unsupported") => Some(StubKind::UnsupportedCodegen),
        _ => None,
    }
}

#[derive(Default)]
struct FirstStringLiteral {
    value: Option<String>,
}

impl<'ast> Visit<'ast> for FirstStringLiteral {
    fn visit_lit_str(&mut self, literal: &'ast LitStr) {
        if self.value.is_none() {
            self.value = Some(normalize_ws(&literal.value()));
        }
    }
}

fn first_string_literal(expr: &Expr) -> Option<String> {
    let mut visitor = FirstStringLiteral::default();
    visitor.visit_expr(expr);
    visitor.value
}

fn source_line(source: &str, span: Span) -> (usize, String) {
    let line = span.start().line;
    let anchor = source
        .lines()
        .nth(line.saturating_sub(1))
        .unwrap_or_else(|| panic!("span line {line} is outside parsed source"))
        .trim()
        .to_owned();
    (line, anchor)
}

struct RuntimeStubVisitor<'source> {
    file: &'source str,
    source: &'source str,
    enabled_kinds: &'source BTreeSet<StubKind>,
    enclosing_items: Vec<String>,
    found: Vec<ScannedMarker>,
}

impl RuntimeStubVisitor<'_> {
    fn with_enclosing_item(&mut self, name: String, visit: impl FnOnce(&mut Self)) {
        self.enclosing_items.push(name);
        visit(self);
        self.enclosing_items.pop();
    }

    fn enclosing_item(&self) -> String {
        self.enclosing_items
            .last()
            .cloned()
            .unwrap_or_else(|| "<none>".to_owned())
    }
}

impl<'ast> Visit<'ast> for RuntimeStubVisitor<'_> {
    fn visit_item(&mut self, item: &'ast Item) {
        if item_attrs(item).is_some_and(is_cfg_test) {
            return;
        }
        visit::visit_item(self, item);
    }

    fn visit_impl_item(&mut self, item: &'ast ImplItem) {
        if impl_item_attrs(item).is_some_and(is_cfg_test) {
            return;
        }
        visit::visit_impl_item(self, item);
    }

    fn visit_trait_item(&mut self, item: &'ast TraitItem) {
        if trait_item_attrs(item).is_some_and(is_cfg_test) {
            return;
        }
        visit::visit_trait_item(self, item);
    }

    fn visit_item_fn(&mut self, item: &'ast syn::ItemFn) {
        self.with_enclosing_item(item.sig.ident.to_string(), |visitor| {
            visit::visit_item_fn(visitor, item);
        });
    }

    fn visit_impl_item_fn(&mut self, item: &'ast syn::ImplItemFn) {
        self.with_enclosing_item(item.sig.ident.to_string(), |visitor| {
            visit::visit_impl_item_fn(visitor, item);
        });
    }

    fn visit_trait_item_fn(&mut self, item: &'ast syn::TraitItemFn) {
        self.with_enclosing_item(item.sig.ident.to_string(), |visitor| {
            visit::visit_trait_item_fn(visitor, item);
        });
    }

    fn visit_expr_call(&mut self, call: &'ast syn::ExprCall) {
        if let Some(kind) = call_kind(&call.func)
            && self.enabled_kinds.contains(&kind)
        {
            let (line, anchor) = source_line(self.source, call.span());
            let payload = call
                .args
                .first()
                .and_then(first_string_literal)
                .unwrap_or_else(|| normalize_ws(&anchor));
            self.found.push(ScannedMarker {
                file: self.file.to_owned(),
                line,
                kind,
                enclosing_item: self.enclosing_item(),
                anchor,
                payload,
            });
        }
        visit::visit_expr_call(self, call);
    }
}

fn collect_literal_ranges(tokens: TokenStream, ranges: &mut Vec<std::ops::Range<usize>>) {
    for token in tokens {
        match token {
            TokenTree::Group(group) => collect_literal_ranges(group.stream(), ranges),
            TokenTree::Literal(literal) => ranges.push(literal.span().byte_range()),
            TokenTree::Ident(_) | TokenTree::Punct(_) => {}
        }
    }
}

fn scan_todo_placeholders(file: &str, source: &str) -> Vec<ScannedMarker> {
    let tokens = TokenStream::from_str(source)
        .unwrap_or_else(|error| panic!("failed to tokenize {file}: {error}"));
    let mut literal_ranges = Vec::new();
    collect_literal_ranges(tokens, &mut literal_ranges);
    let marker = StubKind::TodoPlaceholder.marker();
    source
        .match_indices(marker)
        .filter(|(offset, _)| !literal_ranges.iter().any(|range| range.contains(offset)))
        .map(|(offset, _)| {
            let line = source[..offset]
                .bytes()
                .filter(|byte| *byte == b'\n')
                .count()
                + 1;
            let anchor = source
                .lines()
                .nth(line - 1)
                .expect("TODO marker line must exist")
                .trim()
                .to_owned();
            ScannedMarker {
                file: file.to_owned(),
                line,
                kind: StubKind::TodoPlaceholder,
                enclosing_item: "<comment>".to_owned(),
                anchor,
                payload: marker.to_owned(),
            }
        })
        .collect()
}

fn scan_source_markers(
    file: &str,
    source: &str,
    enabled_kinds: &BTreeSet<StubKind>,
) -> Vec<ScannedMarker> {
    let syntax = syn::parse_file(source)
        .unwrap_or_else(|error| panic!("failed to parse {file} as Rust source: {error}"));
    let mut visitor = RuntimeStubVisitor {
        file,
        source,
        enabled_kinds,
        enclosing_items: Vec::new(),
        found: Vec::new(),
    };
    visitor.visit_file(&syntax);
    if enabled_kinds.contains(&StubKind::TodoPlaceholder) {
        visitor.found.extend(scan_todo_placeholders(file, source));
    }
    visitor.found.sort_by_key(|marker| marker.line);
    visitor.found
}

/// Parse and scan the metadata-declared source files for runtime-stub
/// constructors. `syn` makes discovery token-aware: comments and literals are
/// not expressions, wrapped calls remain calls, multiple calls on one line are
/// distinct, and only syntactically `#[cfg(test)]` items are excluded.
fn scan_markers(scanned_files: &[String], source_patterns: &[String]) -> Vec<ScannedMarker> {
    let enabled_kinds = declared_stub_kinds(source_patterns);
    let mut found = Vec::new();
    for rel in scanned_files {
        let path = workspace_root().join(rel);
        let content = fs::read_to_string(&path).unwrap_or_else(|error| {
            panic!("failed to read {}: {error}", path.display());
        });
        found.extend(scan_source_markers(rel, &content, &enabled_kinds));
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
    let scanned = multiset(
        scan_markers(&doc.meta.scanned_files, &doc.meta.source_patterns)
            .iter()
            .map(ScannedMarker::fingerprint),
    );
    let inventory = multiset(inventory_fingerprints(&doc));

    // Fingerprints in the scan but missing (or under-counted) in the inventory.
    let mut missing = Vec::new();
    for (fp, scanned_count) in &scanned {
        let inv_count = inventory.get(fp).copied().unwrap_or(0);
        if inv_count < *scanned_count {
            missing.push(format!(
                "{fp} (scanned {scanned_count}, inventory {inv_count})"
            ));
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
            stale.push(format!(
                "{fp} (inventory {inv_count}, scanned {scanned_count})"
            ));
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
    let scanned = scan_markers(&doc.meta.scanned_files, &doc.meta.source_patterns);
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

fn unsupported_only() -> BTreeSet<StubKind> {
    BTreeSet::from([StubKind::UnsupportedCodegen])
}

#[test]
fn fingerprint_is_stable_under_pure_line_drift() {
    let src_a = r#"
fn emit_thing() {
    return Err(CodegenError::Unsupported("widget lowering".to_owned()));
}
"#;
    let src_b = r#"
// a newly inserted comment
fn unrelated() {}

fn emit_thing() {
    // another inserted line
    return Err(CodegenError::Unsupported("widget lowering".to_owned()));
}
"#;
    let a = scan_source_markers("fixture.rs", src_a, &unsupported_only());
    let b = scan_source_markers("fixture.rs", src_b, &unsupported_only());
    assert_eq!(a.len(), 1);
    assert_eq!(b.len(), 1);
    assert_eq!(a[0].fingerprint(), b[0].fingerprint());
    assert_ne!(a[0].line, b[0].line);
}

#[test]
fn fingerprint_detects_payload_change() {
    let before = r#"fn emit_thing() {
        return Err(CodegenError::Unsupported("old message".to_owned()));
    }"#;
    let after = r#"fn emit_thing() {
        return Err(CodegenError::Unsupported("new message".to_owned()));
    }"#;
    let before = scan_source_markers("fixture.rs", before, &unsupported_only());
    let after = scan_source_markers("fixture.rs", after, &unsupported_only());
    assert_ne!(before[0].fingerprint(), after[0].fingerprint());
}

#[test]
fn enclosing_item_disambiguates_identical_anchor_lines() {
    let src = r#"
fn lower_join() {
    return Err(CodegenError::Unsupported("same message".to_owned()));
}
fn lower_subquery() {
    return Err(CodegenError::Unsupported("same message".to_owned()));
}
"#;
    let markers = scan_source_markers("fixture.rs", src, &unsupported_only());
    assert_eq!(markers.len(), 2);
    assert_eq!(markers[0].enclosing_item, "lower_join");
    assert_eq!(markers[1].enclosing_item, "lower_subquery");
    assert_ne!(markers[0].fingerprint(), markers[1].fingerprint());
}

#[test]
fn wrapped_marker_and_multiline_payload_are_captured() {
    let src = r#"
fn emit() {
    return Err(CodegenError
        ::Unsupported
        (format!(
            "table {} has {} columns",
            name, count,
        )));
}
"#;
    let markers = scan_source_markers("fixture.rs", src, &unsupported_only());
    assert_eq!(markers.len(), 1);
    assert_eq!(markers[0].enclosing_item, "emit");
    assert_eq!(markers[0].payload, "table {} has {} columns");
}

#[test]
fn marker_text_in_comments_and_strings_is_ignored() {
    let src = r#"
const TEXT: &str = "CodegenError::Unsupported(\"not code\")";
// CodegenError::Unsupported("also not code")
/* CodegenError::Unsupported("still not code") */
fn live() {
    return Err(CodegenError::Unsupported("real marker".to_owned()));
}
"#;
    let markers = scan_source_markers("fixture.rs", src, &unsupported_only());
    assert_eq!(markers.len(), 1);
    assert_eq!(markers[0].payload, "real marker");
}

#[test]
fn multiple_same_kind_markers_on_one_line_are_distinct() {
    let src = r#"fn live() { let _ = (CodegenError::Unsupported("left".into()), CodegenError::Unsupported("right".into())); }"#;
    let markers = scan_source_markers("fixture.rs", src, &unsupported_only());
    assert_eq!(markers.len(), 2);
    assert_eq!(markers[0].line, markers[1].line);
    assert_eq!(markers[0].payload, "left");
    assert_eq!(markers[1].payload, "right");
}

#[test]
fn cfg_test_handling_is_syntactic_and_item_scoped() {
    let src = r##"
const TEXT: &str = "#[cfg(test)]";
// #[cfg(test)]
fn before() { let _ = CodegenError::Unsupported("before".into()); }
#[cfg(test)]
fn test_only() { let _ = CodegenError::Unsupported("test-only".into()); }
fn after() { let _ = CodegenError::Unsupported("after".into()); }
"##;
    let markers = scan_source_markers("fixture.rs", src, &unsupported_only());
    assert_eq!(
        markers
            .iter()
            .map(|marker| marker.payload.as_str())
            .collect::<Vec<_>>(),
        ["before", "after"]
    );
}

#[test]
fn todo_placeholder_in_a_string_is_not_a_comment_marker() {
    let src = r#"
const TEXT: &str = "TODO: Apply collation from P4 if present.";
// TODO: Apply collation from P4 if present.
"#;
    let markers = scan_source_markers(
        "fixture.rs",
        src,
        &BTreeSet::from([StubKind::TodoPlaceholder]),
    );
    assert_eq!(markers.len(), 1);
    assert_eq!(markers[0].line, 3);
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
    let scanned = scan_markers(&scan_files, &existing.meta.source_patterns);

    let mut out = String::new();
    out.push_str("[meta]\n");
    out.push_str("schema_version = \"1.1.0\"\n");
    out.push_str(&format!("bead_id = \"{}\"\n", existing.meta.bead_id));
    out.push_str(&format!("track_id = \"{}\"\n", existing.meta.track_id));
    out.push_str(&format!(
        "sqlite_target = \"{}\"\n",
        existing.meta.sqlite_target
    ));
    out.push_str(&format!(
        "generated_at = \"{}\"\n",
        existing.meta.generated_at
    ));
    out.push_str(&format!(
        "contract_owner = \"{}\"\n",
        existing.meta.contract_owner
    ));
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
    out.push_str(&format!("next_active_id = {}\n", scanned.len() + 1));

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
        out.push_str(&format!(
            "stub_id = \"{}\"\n",
            toml_escape(&resolved.stub_id)
        ));
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
