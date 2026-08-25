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
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::visit::{self, Visit};
use syn::{Attribute, Expr, ImplItem, Item, Lit, LitStr, Meta, Token, TraitItem};

const BEAD_ID: &str = "bd-2yqp6.4.1";
const IDENTITY_MODEL: &str = "fingerprint (file, kind, enclosing_item, normalized_payload); line is a hint (bd-y7otm/GH#136 item 2)";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct InventoryDocument {
    meta: InventoryMeta,
    runtime_stubs: Vec<RuntimeStub>,
    resolved_runtime_stubs: Vec<ResolvedRuntimeStub>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct InventoryMeta {
    schema_version: String,
    bead_id: String,
    track_id: String,
    sqlite_target: String,
    generated_at: String,
    contract_owner: String,
    inventory_scope: String,
    identity_model: String,
    source_patterns: Vec<String>,
    parity_critical_severities: Vec<String>,
    // bd-y7otm: next allocatable active ordinal, so additions never reuse a
    // retired id and never force a renumber of existing entries. `#[serde(default)]`
    // only so the pre-migration TOML can be loaded by the regenerator; the gate
    // asserts `next_active_id > 0`, so the emitted contract must carry it.
    #[serde(default)]
    next_active_id: u32,
    // Files scanned for markers; drives the scanner instead of a hardcoded list.
    // `#[serde(default)]` for the same bootstrap reason; the gate requires exact
    // agreement with the independently declared required scan scope below.
    #[serde(default)]
    scanned_files: Vec<String>,
}

/// Required scan list. This is both the bootstrap fallback for a pre-migration
/// inventory and the independent, fail-closed authority for metadata changes.
/// Adding or removing a scanned file therefore requires an explicit code review.
const REQUIRED_SCANNED_FILES: [&str; 4] = [
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
#[serde(deny_unknown_fields)]
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
    /// Fingerprint component: nearest enclosing function or method.
    /// `#[serde(default)]` only for bootstrap loading; the gate asserts it is
    /// non-empty.
    #[serde(default)]
    enclosing_item: String,
    /// Fingerprint component: normalized diagnostic payload. `#[serde(default)]`
    /// only for bootstrap loading; the gate asserts it is non-empty.
    #[serde(default)]
    payload: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
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

struct RegeneratedEntry<'a> {
    marker: &'a ScannedMarker,
    existing: Option<&'a RuntimeStub>,
    stub_id: String,
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

fn active_id_ordinal(stub_id: &str) -> Option<u32> {
    let digits = stub_id.strip_prefix("RSTUB-ACTIVE-")?;
    (digits.len() == 4 && digits.bytes().all(|byte| byte.is_ascii_digit()))
        .then(|| digits.parse().ok())
        .flatten()
}

fn stable_id_has_valid_format(stub_id: &str) -> bool {
    if active_id_ordinal(stub_id).is_some() {
        return true;
    }
    let Some(digits) = stub_id.strip_prefix("RSTUB-") else {
        return false;
    };
    digits.len() == 4 && digits.bytes().all(|byte| byte.is_ascii_digit())
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

/// Collapse runs of whitespace to single spaces and trim.
fn normalize_ws(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Normalize runtime diagnostic whitespace without discarding semantic
/// characters. Rust already removes source-level string-continuation slashes
/// when `LitStr::value` is decoded, so dropping standalone `\` characters here
/// would make distinct runtime messages share a fingerprint.
fn normalize_payload(s: &str) -> String {
    normalize_ws(s)
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

fn validate_scan_scope(meta: &InventoryMeta) {
    let declared_kinds = declared_stub_kinds(&meta.source_patterns);
    let required_kinds = StubKind::all().into_iter().collect();
    assert_eq!(
        declared_kinds, required_kinds,
        "runtime-stub source-pattern metadata must declare every supported marker kind"
    );

    let declared_files: BTreeSet<&str> = meta.scanned_files.iter().map(String::as_str).collect();
    assert_eq!(
        declared_files.len(),
        meta.scanned_files.len(),
        "runtime-stub scanned-files metadata must not contain duplicates"
    );
    let required_files: BTreeSet<&str> = REQUIRED_SCANNED_FILES.into_iter().collect();
    assert_eq!(
        declared_files, required_files,
        "runtime-stub scanned-files metadata must exactly match the independently reviewed scan scope"
    );
}

#[derive(Clone, Copy)]
struct CfgPossibilities {
    can_be_false: bool,
    can_be_true: bool,
}

/// Conservatively evaluate a cfg expression with `test = false` while treating
/// every other predicate as unknown. Over-approximating unknown predicates is
/// intentional: the scanner may include dead code, but it must never hide code
/// that could be compiled in a production configuration.
fn cfg_possibilities_without_test(meta: &Meta) -> CfgPossibilities {
    match meta {
        Meta::Path(path) if path.is_ident("test") => CfgPossibilities {
            can_be_false: true,
            can_be_true: false,
        },
        Meta::List(list) if list.path.is_ident("all") || list.path.is_ident("any") => {
            let terms = list
                .parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)
                .unwrap_or_else(|error| panic!("failed to parse cfg expression: {error}"));
            if list.path.is_ident("all") {
                terms.iter().fold(
                    CfgPossibilities {
                        can_be_false: false,
                        can_be_true: true,
                    },
                    |possibilities, term| {
                        let term = cfg_possibilities_without_test(term);
                        CfgPossibilities {
                            can_be_false: possibilities.can_be_false || term.can_be_false,
                            can_be_true: possibilities.can_be_true && term.can_be_true,
                        }
                    },
                )
            } else {
                terms.iter().fold(
                    CfgPossibilities {
                        can_be_false: true,
                        can_be_true: false,
                    },
                    |possibilities, term| {
                        let term = cfg_possibilities_without_test(term);
                        CfgPossibilities {
                            can_be_false: possibilities.can_be_false && term.can_be_false,
                            can_be_true: possibilities.can_be_true || term.can_be_true,
                        }
                    },
                )
            }
        }
        Meta::List(list) if list.path.is_ident("not") => {
            let terms = list
                .parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)
                .unwrap_or_else(|error| panic!("failed to parse cfg expression: {error}"));
            assert_eq!(
                terms.len(),
                1,
                "cfg(not(...)) must contain exactly one expression"
            );
            let inner = cfg_possibilities_without_test(
                terms
                    .first()
                    .expect("cfg(not(...)) expression count was checked"),
            );
            CfgPossibilities {
                can_be_false: inner.can_be_true,
                can_be_true: inner.can_be_false,
            }
        }
        // Unknown predicates stay conservatively in scope.
        Meta::Path(_) | Meta::List(_) | Meta::NameValue(_) => CfgPossibilities {
            can_be_false: true,
            can_be_true: true,
        },
    }
}

fn cfg_expression_requires_test(meta: &Meta) -> bool {
    !cfg_possibilities_without_test(meta).can_be_true
}

fn is_cfg_test(attrs: &[Attribute]) -> bool {
    attrs.iter().any(|attr| {
        if !attr.path().is_ident("cfg") {
            return false;
        }
        let terms = attr
            .parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)
            .unwrap_or_else(|error| panic!("failed to parse cfg attribute: {error}"));
        terms.len() == 1 && terms.first().is_some_and(cfg_expression_requires_test)
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
        _ => None,
    }
}

fn impl_item_attrs(item: &ImplItem) -> Option<&[Attribute]> {
    match item {
        ImplItem::Const(item) => Some(&item.attrs),
        ImplItem::Fn(item) => Some(&item.attrs),
        ImplItem::Type(item) => Some(&item.attrs),
        ImplItem::Macro(item) => Some(&item.attrs),
        _ => None,
    }
}

fn trait_item_attrs(item: &TraitItem) -> Option<&[Attribute]> {
    match item {
        TraitItem::Const(item) => Some(&item.attrs),
        TraitItem::Fn(item) => Some(&item.attrs),
        TraitItem::Type(item) => Some(&item.attrs),
        TraitItem::Macro(item) => Some(&item.attrs),
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

fn leading_string_literal_in_tokens(tokens: TokenStream) -> Option<String> {
    let TokenTree::Literal(literal) = tokens.into_iter().next()? else {
        return None;
    };
    let literal = syn::parse_str::<LitStr>(&literal.to_string()).ok()?;
    Some(normalize_payload(&literal.value()))
}

/// Extract the diagnostic template only from expression shapes whose runtime
/// value is directly derived from that template. An arbitrary nested literal
/// inside a helper call is not the diagnostic and must fall back to the whole
/// argument source instead.
fn direct_diagnostic_literal(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Lit(expr) => match &expr.lit {
            Lit::Str(literal) => Some(normalize_payload(&literal.value())),
            _ => None,
        },
        Expr::Paren(expr) => direct_diagnostic_literal(&expr.expr),
        Expr::Group(expr) => direct_diagnostic_literal(&expr.expr),
        Expr::Reference(expr) => direct_diagnostic_literal(&expr.expr),
        Expr::MethodCall(expr)
            if expr.args.is_empty()
                && (expr.method == "into"
                    || expr.method == "to_owned"
                    || expr.method == "to_string") =>
        {
            direct_diagnostic_literal(&expr.receiver)
        }
        Expr::Macro(expr)
            if expr.mac.path.segments.last().is_some_and(|segment| {
                segment.ident == "format" || segment.ident == "format_args"
            }) =>
        {
            leading_string_literal_in_tokens(expr.mac.tokens.clone())
        }
        _ => None,
    }
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
    test_only_ranges: Vec<std::ops::Range<usize>>,
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
            self.test_only_ranges.push(item.span().byte_range());
            return;
        }
        visit::visit_item(self, item);
    }

    fn visit_impl_item(&mut self, item: &'ast ImplItem) {
        if impl_item_attrs(item).is_some_and(is_cfg_test) {
            self.test_only_ranges.push(item.span().byte_range());
            return;
        }
        visit::visit_impl_item(self, item);
    }

    fn visit_trait_item(&mut self, item: &'ast TraitItem) {
        if trait_item_attrs(item).is_some_and(is_cfg_test) {
            self.test_only_ranges.push(item.span().byte_range());
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
            let payload = call.args.first().map_or_else(
                || normalize_ws(&anchor),
                |argument| {
                    direct_diagnostic_literal(argument).unwrap_or_else(|| {
                        let range = argument.span().byte_range();
                        let argument_source = self.source.get(range.clone()).unwrap_or_else(|| {
                            panic!("argument span {range:?} is outside parsed source")
                        });
                        normalize_ws(argument_source)
                    })
                },
            );
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

fn scan_todo_placeholders(
    file: &str,
    source: &str,
    test_only_ranges: &[std::ops::Range<usize>],
) -> Vec<ScannedMarker> {
    let tokens = TokenStream::from_str(source)
        .unwrap_or_else(|error| panic!("failed to tokenize {file}: {error}"));
    let mut literal_ranges = Vec::new();
    collect_literal_ranges(tokens, &mut literal_ranges);
    let marker = StubKind::TodoPlaceholder.marker();
    source
        .match_indices(marker)
        .filter(|(offset, _)| {
            !literal_ranges.iter().any(|range| range.contains(offset))
                && !test_only_ranges.iter().any(|range| range.contains(offset))
        })
        .map(|(offset, _)| {
            let prefix = source
                .get(..offset)
                .expect("match_indices must return a UTF-8 boundary");
            let line = prefix.bytes().filter(|byte| *byte == b'\n').count() + 1;
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
        test_only_ranges: Vec::new(),
        found: Vec::new(),
    };
    visitor.visit_file(&syntax);
    if enabled_kinds.contains(&StubKind::TodoPlaceholder) {
        visitor.found.extend(scan_todo_placeholders(
            file,
            source,
            &visitor.test_only_ranges,
        ));
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
            payload: normalize_payload(&stub.payload),
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

fn reconcile_stable_ids<'a>(
    existing: &'a InventoryDocument,
    scanned: &'a [ScannedMarker],
) -> (Vec<RegeneratedEntry<'a>>, u32) {
    let mut old_by_fingerprint: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    for (index, stub) in existing.runtime_stubs.iter().enumerate() {
        let fingerprint = Fingerprint {
            file: stub.file.clone(),
            kind: stub.kind,
            enclosing_item: stub.enclosing_item.clone(),
            payload: normalize_payload(&stub.payload),
        };
        old_by_fingerprint
            .entry(fingerprint.render())
            .or_default()
            .push(index);
    }

    let mut new_by_fingerprint: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    for (index, marker) in scanned.iter().enumerate() {
        new_by_fingerprint
            .entry(marker.fingerprint().render())
            .or_default()
            .push(index);
    }

    let mut matched: Vec<Option<&RuntimeStub>> = vec![None; scanned.len()];
    let mut disappeared = Vec::new();
    for (fingerprint, old_indices) in &old_by_fingerprint {
        let Some(new_indices) = new_by_fingerprint.get(fingerprint) else {
            disappeared.push(format!(
                "{fingerprint} (ids {:?})",
                old_indices
                    .iter()
                    .map(|index| existing.runtime_stubs[*index].stub_id.as_str())
                    .collect::<Vec<_>>()
            ));
            continue;
        };
        assert_eq!(
            old_indices.len(),
            new_indices.len(),
            "runtime-stub fingerprint multiplicity changed; explicit review is required for {fingerprint} (old {}, new {})",
            old_indices.len(),
            new_indices.len()
        );

        if old_indices.len() == 1 {
            matched[new_indices[0]] = Some(&existing.runtime_stubs[old_indices[0]]);
            continue;
        }

        // A duplicated semantic fingerprint cannot be paired safely after line
        // drift. Preserve IDs only when every old hint still identifies one
        // unique current occurrence; otherwise fail for explicit review.
        let mut old_by_line = BTreeMap::new();
        for old_index in old_indices {
            let old = &existing.runtime_stubs[*old_index];
            assert!(
                old_by_line.insert(old.line, old).is_none(),
                "ambiguous duplicate fingerprint {fingerprint}: multiple old IDs share line {}",
                old.line
            );
        }
        for new_index in new_indices {
            let marker = &scanned[*new_index];
            let old = old_by_line.remove(&marker.line).unwrap_or_else(|| {
                panic!(
                    "ambiguous duplicate fingerprint {fingerprint} moved away from its line hints; explicit old-to-new ID mapping is required"
                )
            });
            matched[*new_index] = Some(old);
        }
    }
    assert!(
        disappeared.is_empty(),
        "existing runtime-stub fingerprints disappeared; resolve each explicitly instead of silently retiring or renumbering: {disappeared:#?}"
    );

    let mut used_ids: BTreeSet<String> = existing
        .runtime_stubs
        .iter()
        .map(|stub| stub.stub_id.clone())
        .chain(
            existing
                .resolved_runtime_stubs
                .iter()
                .map(|stub| stub.stub_id.clone()),
        )
        .collect();
    assert_eq!(
        used_ids.len(),
        existing.runtime_stubs.len() + existing.resolved_runtime_stubs.len(),
        "existing active/resolved stable IDs must be globally unique before regeneration"
    );
    let mut next_active_id = existing.meta.next_active_id;
    let mut entries = Vec::with_capacity(scanned.len());
    for (index, marker) in scanned.iter().enumerate() {
        if let Some(old) = matched[index] {
            entries.push(RegeneratedEntry {
                marker,
                existing: Some(old),
                stub_id: old.stub_id.clone(),
            });
            continue;
        }

        let stub_id = format!("RSTUB-ACTIVE-{next_active_id:04}");
        assert!(
            used_ids.insert(stub_id.clone()),
            "meta.next_active_id would reuse existing stable ID {stub_id}"
        );
        next_active_id = next_active_id
            .checked_add(1)
            .expect("active runtime-stub ID space exhausted");
        entries.push(RegeneratedEntry {
            marker,
            existing: None,
            stub_id,
        });
    }
    let existing_order: BTreeMap<&str, usize> = existing
        .runtime_stubs
        .iter()
        .enumerate()
        .map(|(index, stub)| (stub.stub_id.as_str(), index))
        .collect();
    entries.sort_by_key(|entry| {
        existing_order
            .get(entry.stub_id.as_str())
            .copied()
            .unwrap_or_else(|| {
                let ordinal = active_id_ordinal(&entry.stub_id)
                    .and_then(|value| usize::try_from(value).ok())
                    .unwrap_or(usize::MAX);
                existing.runtime_stubs.len().saturating_add(ordinal)
            })
    });
    (entries, next_active_id)
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
    assert_eq!(doc.meta.inventory_scope, "runtime-items-excluding-cfg-test");
    assert_eq!(doc.meta.identity_model, IDENTITY_MODEL);
    assert!(!doc.meta.parity_critical_severities.is_empty());
    assert!(doc.meta.next_active_id > 0);
    validate_scan_scope(&doc.meta);
}

#[test]
fn scan_scope_metadata_cannot_self_modify() {
    for kind in StubKind::all() {
        let mut missing_kind = load_inventory();
        missing_kind
            .meta
            .source_patterns
            .retain(|pattern| pattern != kind.marker());
        assert!(
            std::panic::catch_unwind(|| validate_scan_scope(&missing_kind.meta)).is_err(),
            "dropping supported marker kind {kind:?} must fail the inventory contract"
        );
    }

    for required_file in REQUIRED_SCANNED_FILES {
        let mut missing_file = load_inventory();
        missing_file
            .meta
            .scanned_files
            .retain(|file| file != required_file);
        assert!(
            std::panic::catch_unwind(|| validate_scan_scope(&missing_file.meta)).is_err(),
            "dropping required source file {required_file:?} must fail the inventory contract"
        );
    }

    let mut duplicate_file = load_inventory();
    duplicate_file
        .meta
        .scanned_files
        .push(REQUIRED_SCANNED_FILES[0].to_owned());
    assert!(
        std::panic::catch_unwind(|| validate_scan_scope(&duplicate_file.meta)).is_err(),
        "duplicating a required source file must fail the inventory contract"
    );

    let mut unreviewed_file = load_inventory();
    unreviewed_file
        .meta
        .scanned_files
        .push("crates/fsqlite-core/src/lib.rs".to_owned());
    assert!(
        std::panic::catch_unwind(|| validate_scan_scope(&unreviewed_file.meta)).is_err(),
        "adding an unreviewed source file must fail the inventory contract"
    );
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
        let ordinal = active_id_ordinal(&stub.stub_id).unwrap_or_else(|| {
            panic!(
                "active stub_id must use RSTUB-ACTIVE-NNNN format: {}",
                stub.stub_id
            )
        });
        assert!(
            seen_stub_ids.insert(stub.stub_id.as_str()),
            "duplicate stub_id: {}",
            stub.stub_id
        );
        max_active_ordinal = max_active_ordinal.max(ordinal);

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

    for stub in &doc.resolved_runtime_stubs {
        assert!(
            stable_id_has_valid_format(&stub.stub_id),
            "resolved stub_id has invalid stable-ID format: {}",
            stub.stub_id
        );
        if let Some(ordinal) = active_id_ordinal(&stub.stub_id) {
            max_active_ordinal = max_active_ordinal.max(ordinal);
        }
        assert!(
            seen_stub_ids.insert(stub.stub_id.as_str()),
            "duplicate stub_id across active and resolved entries: {}",
            stub.stub_id
        );
        match (&stub.superseded_stub_id, &stub.identity_note) {
            (Some(superseded), Some(note)) => {
                assert!(
                    stable_id_has_valid_format(superseded),
                    "{} has invalid superseded stable-ID format: {superseded}",
                    stub.stub_id
                );
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

    assert!(
        doc.meta.next_active_id > max_active_ordinal,
        "meta.next_active_id ({}) must exceed every active-format ID in active and resolved history ({max_active_ordinal})",
        doc.meta.next_active_id
    );
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
            payload: normalize_payload(&stub.payload),
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
fn fingerprint_preserves_semantic_backslash() {
    let with_backslash = r#"fn emit_thing() {
        return Err(CodegenError::Unsupported("path \\ segment".to_owned()));
    }"#;
    let with_source_continuation = r#"fn emit_thing() {
        return Err(CodegenError::Unsupported("path \
            segment".to_owned()));
    }"#;
    let without_backslash = r#"fn emit_thing() {
        return Err(CodegenError::Unsupported("path segment".to_owned()));
    }"#;
    let with_backslash = scan_source_markers("fixture.rs", with_backslash, &unsupported_only());
    let with_source_continuation =
        scan_source_markers("fixture.rs", with_source_continuation, &unsupported_only());
    let without_backslash =
        scan_source_markers("fixture.rs", without_backslash, &unsupported_only());
    assert_eq!(with_backslash[0].payload, r"path \ segment");
    assert_eq!(with_source_continuation[0].payload, "path segment");
    assert_eq!(
        with_source_continuation[0].fingerprint(),
        without_backslash[0].fingerprint(),
        "a Rust source-level string continuation must not enter runtime-stub identity"
    );
    assert_ne!(
        with_backslash[0].fingerprint(),
        without_backslash[0].fingerprint(),
        "a semantic backslash must participate in runtime-stub identity"
    );
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
fn nested_literal_in_computed_payload_uses_the_whole_argument() {
    let src = r#"
fn live() {
    return Err(CodegenError::Unsupported(build_message("prefix", detail)));
}
"#;
    let markers = scan_source_markers("fixture.rs", src, &unsupported_only());
    assert_eq!(markers.len(), 1);
    assert_eq!(markers[0].payload, "build_message(\"prefix\", detail)");
}

#[test]
fn nested_literal_in_a_computed_format_template_is_not_the_payload() {
    let src = r#"
fn live() {
    return Err(CodegenError::Unsupported(format!(concat!("prefix", "{detail}"), detail = detail)));
}
"#;
    let markers = scan_source_markers("fixture.rs", src, &unsupported_only());
    assert_eq!(markers.len(), 1);
    assert!(markers[0].payload.starts_with("format!(concat!"));
    assert_ne!(markers[0].payload, "prefix");
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
#[cfg(all(test, unix))]
fn test_only_on_unix() { let _ = CodegenError::Unsupported("test-only-unix".into()); }
#[cfg(any(test, feature = "fixture-live"))]
fn live_in_a_feature_build() { let _ = CodegenError::Unsupported("feature-or-test".into()); }
#[cfg(not(test))]
fn live_outside_tests() { let _ = CodegenError::Unsupported("not-test".into()); }
#[cfg(not(not(test)))]
fn nested_test_only() { let _ = CodegenError::Unsupported("nested-test-only".into()); }
#[cfg(not(any(not(test), feature = "fixture-live")))]
fn nested_test_only_when_feature_is_off() { let _ = CodegenError::Unsupported("nested-test-only-feature-off".into()); }
#[cfg(not(all(test, feature = "fixture-live")))]
fn live_when_test_is_off() { let _ = CodegenError::Unsupported("not-all-test-feature".into()); }
fn after() { let _ = CodegenError::Unsupported("after".into()); }
"##;
    let markers = scan_source_markers("fixture.rs", src, &unsupported_only());
    assert_eq!(
        markers
            .iter()
            .map(|marker| marker.payload.as_str())
            .collect::<Vec<_>>(),
        [
            "before",
            "feature-or-test",
            "not-test",
            "not-all-test-feature",
            "after"
        ]
    );
}

#[test]
fn todo_placeholder_in_a_string_is_not_a_comment_marker() {
    let src = r#"
const TEXT: &str = "TODO: Apply collation from P4 if present.";
// TODO: Apply collation from P4 if present.
#[cfg(test)]
fn test_only() {
    // TODO: Apply collation from P4 if present.
}
"#;
    let markers = scan_source_markers(
        "fixture.rs",
        src,
        &BTreeSet::from([StubKind::TodoPlaceholder]),
    );
    assert_eq!(markers.len(), 1);
    assert_eq!(markers[0].line, 3);
}

fn reconciliation_fixture(stubs: Vec<RuntimeStub>, next_active_id: u32) -> InventoryDocument {
    InventoryDocument {
        meta: InventoryMeta {
            schema_version: "1.1.0".to_owned(),
            bead_id: BEAD_ID.to_owned(),
            track_id: "bd-2yqp6.4".to_owned(),
            sqlite_target: "3.52.0".to_owned(),
            generated_at: "fixture".to_owned(),
            contract_owner: "fixture".to_owned(),
            inventory_scope: "runtime-items-excluding-cfg-test".to_owned(),
            identity_model: IDENTITY_MODEL.to_owned(),
            source_patterns: vec![StubKind::UnsupportedCodegen.marker().to_owned()],
            parity_critical_severities: vec!["critical".to_owned()],
            next_active_id,
            scanned_files: vec!["fixture.rs".to_owned()],
        },
        runtime_stubs: stubs,
        resolved_runtime_stubs: Vec::new(),
    }
}

fn reconciliation_stub(stub_id: &str, line: usize, payload: &str) -> RuntimeStub {
    RuntimeStub {
        stub_id: stub_id.to_owned(),
        file: "fixture.rs".to_owned(),
        line,
        kind: StubKind::UnsupportedCodegen,
        kind_description: "fixture".to_owned(),
        severity: "critical".to_owned(),
        feature_id: "SURF-SQL-CORE-001".to_owned(),
        owner: "fixture".to_owned(),
        closure_strategy: "implement".to_owned(),
        anchor: "fixture".to_owned(),
        enclosing_item: "emit".to_owned(),
        payload: payload.to_owned(),
    }
}

fn reconciliation_marker(line: usize, payload: &str) -> ScannedMarker {
    ScannedMarker {
        file: "fixture.rs".to_owned(),
        line,
        kind: StubKind::UnsupportedCodegen,
        enclosing_item: "emit".to_owned(),
        anchor: "fixture".to_owned(),
        payload: payload.to_owned(),
    }
}

#[test]
fn stable_id_reconciliation_preserves_ids_and_allocates_monotonically() {
    let existing = reconciliation_fixture(
        vec![reconciliation_stub("RSTUB-ACTIVE-0042", 10, "existing")],
        94,
    );
    let scanned = vec![
        reconciliation_marker(20, "existing"),
        reconciliation_marker(30, "new"),
    ];
    let (entries, next_active_id) = reconcile_stable_ids(&existing, &scanned);
    assert_eq!(
        entries
            .iter()
            .map(|entry| entry.stub_id.as_str())
            .collect::<Vec<_>>(),
        ["RSTUB-ACTIVE-0042", "RSTUB-ACTIVE-0094"]
    );
    assert_eq!(next_active_id, 95);
}

#[test]
fn duplicate_fingerprint_line_drift_requires_explicit_mapping() {
    let existing = reconciliation_fixture(
        vec![
            reconciliation_stub("RSTUB-ACTIVE-0042", 10, "duplicate"),
            reconciliation_stub("RSTUB-ACTIVE-0043", 20, "duplicate"),
        ],
        94,
    );
    let scanned = vec![
        reconciliation_marker(30, "duplicate"),
        reconciliation_marker(40, "duplicate"),
    ];
    let result = std::panic::catch_unwind(|| reconcile_stable_ids(&existing, &scanned));
    assert!(
        result.is_err(),
        "duplicate fingerprints must not be paired by source order after line drift"
    );
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

fn render_inventory_toml(existing: &InventoryDocument) -> String {
    // Preserve existing metadata, classifications, IDs, and resolved history;
    // refresh source hints and append only genuinely new fingerprints.
    let scan_files: Vec<String> = if existing.meta.scanned_files.is_empty() {
        REQUIRED_SCANNED_FILES
            .iter()
            .map(|s| (*s).to_owned())
            .collect()
    } else {
        existing.meta.scanned_files.clone()
    };
    let scanned = scan_markers(&scan_files, &existing.meta.source_patterns);
    let (entries, next_active_id) = reconcile_stable_ids(existing, &scanned);

    let mut out = String::new();
    out.push_str("[meta]\n");
    out.push_str(&format!(
        "schema_version = \"{}\"\n",
        toml_escape(&existing.meta.schema_version)
    ));
    out.push_str(&format!(
        "bead_id = \"{}\"\n",
        toml_escape(&existing.meta.bead_id)
    ));
    out.push_str(&format!(
        "track_id = \"{}\"\n",
        toml_escape(&existing.meta.track_id)
    ));
    out.push_str(&format!(
        "sqlite_target = \"{}\"\n",
        toml_escape(&existing.meta.sqlite_target)
    ));
    out.push_str(&format!(
        "generated_at = \"{}\"\n",
        toml_escape(&existing.meta.generated_at)
    ));
    out.push_str(&format!(
        "contract_owner = \"{}\"\n",
        toml_escape(&existing.meta.contract_owner)
    ));
    out.push_str(&format!(
        "inventory_scope = \"{}\"\n",
        toml_escape(&existing.meta.inventory_scope)
    ));
    out.push_str(&format!(
        "identity_model = \"{}\"\n",
        toml_escape(&existing.meta.identity_model)
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
    out.push_str(&format!("next_active_id = {next_active_id}\n"));

    // Existing entries remain in document order and keep stable IDs and
    // classifications. Genuinely new fingerprints are appended and consume
    // `meta.next_active_id`.
    for entry in entries {
        let marker = entry.marker;
        let kind_description = entry.existing.map_or_else(
            || match marker.kind {
                StubKind::NotImplemented => {
                    "FrankenError::NotImplemented runtime fallback".to_owned()
                }
                StubKind::UnsupportedCodegen => {
                    "CodegenError::Unsupported compiler fallback".to_owned()
                }
                StubKind::TodoPlaceholder => "runtime TODO placeholder".to_owned(),
            },
            |stub| stub.kind_description.clone(),
        );
        let severity = entry
            .existing
            .map_or("critical", |stub| stub.severity.as_str());
        let feature_id = entry
            .existing
            .map_or("SURF-SQL-CORE-001", |stub| stub.feature_id.as_str());
        let owner = entry
            .existing
            .map_or("track-d-engine-runtime", |stub| stub.owner.as_str());
        let closure_strategy = entry
            .existing
            .map_or("implement", |stub| stub.closure_strategy.as_str());

        out.push_str("\n[[runtime_stubs]]\n");
        out.push_str(&format!("stub_id = \"{}\"\n", entry.stub_id));
        out.push_str(&format!("file = \"{}\"\n", toml_escape(&marker.file)));
        out.push_str(&format!("line = {}\n", marker.line));
        out.push_str(&format!("kind = \"{}\"\n", kind_str(marker.kind)));
        out.push_str(&format!(
            "kind_description = \"{}\"\n",
            toml_escape(&kind_description)
        ));
        out.push_str(&format!("severity = \"{}\"\n", toml_escape(severity)));
        out.push_str(&format!("feature_id = \"{}\"\n", toml_escape(feature_id)));
        out.push_str(&format!("owner = \"{}\"\n", toml_escape(owner)));
        out.push_str(&format!(
            "closure_strategy = \"{}\"\n",
            toml_escape(closure_strategy)
        ));
        out.push_str(&format!("anchor = \"{}\"\n", toml_escape(&marker.anchor)));
        out.push_str(&format!(
            "enclosing_item = \"{}\"\n",
            toml_escape(&marker.enclosing_item)
        ));
        let payload = entry
            .existing
            .map_or(marker.payload.as_str(), |stub| stub.payload.as_str());
        out.push_str(&format!("payload = \"{}\"\n", toml_escape(payload)));
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

    out
}

#[test]
#[ignore = "operator-run regenerator; prints the canonical inventory TOML"]
fn regenerate_inventory_toml() {
    let out = render_inventory_toml(&load_inventory());

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
