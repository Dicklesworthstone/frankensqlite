use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use serde::Deserialize;
use syn::parse::{ParseStream, Parser};
use syn::spanned::Spanned;
use syn::visit::Visit;
use syn::{Attribute, Expr, Item, Lit, Meta, Token};

const BEAD_ID: &str = "bd-16e7";
const LOG_PREFIX: &str = "[REGR_GUARD]";
const REGRESSION_BASELINE_PATH: &str = "tests/regression_baseline.json";
// The canonical workspace run and every opt-in release run are captured at
// `tested_commit`. Their immutable artifacts plus this manifest are then
// committed as the only changes in a descendant evidence commit. This avoids
// putting the evidence commit's own hash inside the manifest.
const EVIDENCE_MANIFEST_ENV: &str = "FSQLITE_REGRESSION_GUARD_EVIDENCE_MANIFEST";
const EVIDENCE_MANIFEST_BLAKE3_ENV: &str = "FSQLITE_REGRESSION_GUARD_EVIDENCE_MANIFEST_BLAKE3";
const EVIDENCE_SCHEMA_VERSION: u32 = 1;
const EVIDENCE_PATH_PREFIX: &str = "tests/artifacts/release-evidence/";
const RELEASE_GUARD_LOCATOR: &str = concat!(
    "crates/fsqlite-harness/tests/phase5_regression_guard.rs::",
    "phase5_regression_guard_full_workspace_against_baseline"
);
const CANONICAL_WORKSPACE_TEST_ARGV: &[&str] = &["cargo", "test", "--locked", "--workspace"];
const UNINSPECTED_RUST_SOURCE_PATHS: &[&str] = &["crates/fsqlite-core/src/connection.rs"];
const SOURCE_INVENTORY_SOUNDNESS_LIMITATIONS: &[&str] = &[
    "attribute and opaque item-macro expansions are not compiler-audited",
    "out-of-line modules and include sites do not propagate cfg, identity, or multiplicity",
    "ignored doctest directives are not inventoried by the Rust item collector",
    "active test identities are not baseline-validated, so aggregate counts cannot detect identity substitution",
    "run_for_release prose requirements are not yet represented as machine-auditable typed contracts",
    "run_for_release source-to-Cargo-package mapping is convention-derived rather than metadata-audited",
    "exact-test transcripts do not carry a trusted runner attestation of Cargo package identity",
    "baseline numeric counts lack an immutable command-bound provenance artifact",
];

#[derive(Debug, Clone, PartialEq, Eq)]
struct IgnoredTestSource {
    source_path: String,
    test_name: String,
    reason: String,
    cfg_condition: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RepositoryIgnoreInventory {
    records: Vec<IgnoredTestSource>,
    uninspected_sources: Vec<String>,
    soundness_limitations: Vec<String>,
}

impl IgnoredTestSource {
    fn locator(&self) -> String {
        format!("{}::{}", self.source_path, self.test_name)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum CfgCondition {
    Atom(String),
    All(Vec<Self>),
    Any(Vec<Self>),
    Not(Box<Self>),
}

impl CfgCondition {
    fn all(conditions: impl IntoIterator<Item = Self>) -> Self {
        let mut flattened = Vec::new();
        for condition in conditions {
            match condition {
                Self::All(nested) => flattened.extend(nested),
                other => flattened.push(other),
            }
        }
        flattened.sort_by_key(Self::render);
        flattened.dedup();
        if flattened.len() == 1 {
            flattened.pop().expect("one condition was present")
        } else {
            Self::All(flattened)
        }
    }

    fn any(conditions: impl IntoIterator<Item = Self>) -> Self {
        let mut flattened = Vec::new();
        for condition in conditions {
            match condition {
                Self::Any(nested) => flattened.extend(nested),
                other => flattened.push(other),
            }
        }
        flattened.sort_by_key(Self::render);
        flattened.dedup();
        if flattened.len() == 1 {
            flattened.pop().expect("one condition was present")
        } else {
            Self::Any(flattened)
        }
    }

    fn render(&self) -> String {
        match self {
            Self::Atom(atom) => atom.clone(),
            Self::All(conditions) => format!(
                "all({})",
                conditions
                    .iter()
                    .map(Self::render)
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            Self::Any(conditions) => format!(
                "any({})",
                conditions
                    .iter()
                    .map(Self::render)
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            Self::Not(condition) => format!("not({})", condition.render()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct IgnoreAnnotation {
    reason: String,
    guard: Option<CfgCondition>,
}

fn normalize_source_path(path: &Path) -> Result<String, String> {
    let path_text = path
        .to_str()
        .ok_or_else(|| "source path is not valid UTF-8".to_owned())?
        .replace('\\', "/");
    let bytes = path_text.as_bytes();
    if path_text.starts_with('/')
        || (bytes.len() >= 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':')
    {
        return Err(format!(
            "source path must be repository-relative, found `{path_text}`"
        ));
    }

    let mut components = Vec::new();
    for component in path_text.split('/') {
        match component {
            "" | "." => {}
            ".." => {
                if components.pop().is_none() {
                    return Err(format!(
                        "source path escapes the repository root: `{path_text}`"
                    ));
                }
            }
            normal => components.push(normal),
        }
    }
    if components.is_empty() {
        return Err(format!(
            "source path must identify a file, found `{path_text}`"
        ));
    }
    Ok(components.join("/"))
}

fn canonical_meta_path(path: &syn::Path) -> Result<String, String> {
    if path.leading_colon.is_some() {
        return Err("cfg predicate paths must not start with `::`".to_owned());
    }
    let mut segments = Vec::new();
    for segment in &path.segments {
        if !matches!(segment.arguments, syn::PathArguments::None) {
            return Err(format!(
                "cfg predicate path segment `{}` must not have arguments",
                segment.ident
            ));
        }
        segments.push(segment.ident.to_string());
    }
    if segments.is_empty() {
        return Err("cfg predicate path must not be empty".to_owned());
    }
    Ok(segments.join("::"))
}

fn parse_meta_list(list: &syn::MetaList) -> Result<Vec<Meta>, String> {
    syn::punctuated::Punctuated::<Meta, Token![,]>::parse_terminated
        .parse2(list.tokens.clone())
        .map(|items| items.into_iter().collect())
        .map_err(|error| {
            format!(
                "unable to parse `{}` attribute arguments: {error}",
                list.path.segments.last().map_or_else(
                    || "<unknown>".to_owned(),
                    |segment| segment.ident.to_string()
                )
            )
        })
}

fn canonical_cfg_condition(meta: &Meta) -> Result<CfgCondition, String> {
    match meta {
        Meta::Path(path) => canonical_meta_path(path).map(CfgCondition::Atom),
        Meta::NameValue(name_value) => {
            let key = canonical_meta_path(&name_value.path)?;
            let Expr::Lit(expression) = &name_value.value else {
                return Err(format!(
                    "cfg predicate `{key}` must use a string literal value"
                ));
            };
            let Lit::Str(value) = &expression.lit else {
                return Err(format!(
                    "cfg predicate `{key}` must use a string literal value"
                ));
            };
            Ok(CfgCondition::Atom(format!("{key}={:?}", value.value())))
        }
        Meta::List(list) => {
            let operator = canonical_meta_path(&list.path)?;
            let operands = parse_meta_list(list)?;
            match operator.as_str() {
                "all" => operands
                    .iter()
                    .map(canonical_cfg_condition)
                    .collect::<Result<Vec<_>, _>>()
                    .map(CfgCondition::all),
                "any" => operands
                    .iter()
                    .map(canonical_cfg_condition)
                    .collect::<Result<Vec<_>, _>>()
                    .map(CfgCondition::any),
                "not" => {
                    if operands.len() != 1 {
                        return Err(format!(
                            "cfg `not(...)` requires exactly one predicate, found {}",
                            operands.len()
                        ));
                    }
                    canonical_cfg_condition(&operands[0])
                        .map(Box::new)
                        .map(CfgCondition::Not)
                }
                _ => Err(format!(
                    "unsupported cfg predicate operator `{operator}`; expected all, any, or not"
                )),
            }
        }
    }
}

fn conjoin_cfg(left: Option<CfgCondition>, right: Option<CfgCondition>) -> Option<CfgCondition> {
    match (left, right) {
        (Some(left), Some(right)) => Some(CfgCondition::all([left, right])),
        (Some(condition), None) | (None, Some(condition)) => Some(condition),
        (None, None) => None,
    }
}

fn cfg_gate_from_meta(meta: &Meta) -> Result<Option<CfgCondition>, String> {
    if meta.path().is_ident("cfg") {
        let Meta::List(list) = meta else {
            return Err("cfg must use parenthesized arguments".to_owned());
        };
        let predicates = parse_meta_list(list)?;
        if predicates.len() != 1 {
            return Err(format!(
                "cfg requires exactly one predicate, found {}",
                predicates.len()
            ));
        }
        return canonical_cfg_condition(&predicates[0]).map(Some);
    }

    if !meta.path().is_ident("cfg_attr") {
        return Ok(None);
    }
    let Meta::List(list) = meta else {
        return Err("cfg_attr must use parenthesized arguments".to_owned());
    };
    let arguments = parse_meta_list(list)?;
    if arguments.len() < 2 {
        return Err(format!(
            "cfg_attr requires a condition and at least one attribute, found {} argument(s)",
            arguments.len()
        ));
    }

    let application_guard = canonical_cfg_condition(&arguments[0])?;
    let nested_gates = arguments[1..]
        .iter()
        .map(cfg_gate_from_meta)
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    if nested_gates.is_empty() {
        return Ok(None);
    }

    let nested_gate = CfgCondition::all(nested_gates);
    Ok(Some(CfgCondition::any([
        CfgCondition::Not(Box::new(application_guard)),
        nested_gate,
    ])))
}

fn cfg_gate_from_attributes(attrs: &[Attribute]) -> Result<Option<CfgCondition>, String> {
    let gates = attrs
        .iter()
        .map(|attr| cfg_gate_from_meta(&attr.meta))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    if gates.is_empty() {
        Ok(None)
    } else {
        Ok(Some(CfgCondition::all(gates)))
    }
}

fn effective_cfg_context(
    inherited: Option<CfgCondition>,
    attrs: &[Attribute],
) -> Result<Option<CfgCondition>, String> {
    cfg_gate_from_attributes(attrs).map(|local| conjoin_cfg(inherited, local))
}

fn parse_reasoned_ignore(meta: &Meta) -> Result<String, String> {
    let Meta::NameValue(name_value) = meta else {
        return Err("ignore attributes require a reason: use `#[ignore = \"reason\"]`".to_owned());
    };
    let Expr::Lit(expression) = &name_value.value else {
        return Err("ignore reason must be a string literal".to_owned());
    };
    let Lit::Str(reason) = &expression.lit else {
        return Err("ignore reason must be a string literal".to_owned());
    };
    let reason = reason.value();
    if reason.trim().is_empty() {
        return Err("ignore reason must not be empty or whitespace-only".to_owned());
    }
    if reason.trim() != reason {
        return Err("ignore reason must not have leading or trailing whitespace".to_owned());
    }
    Ok(reason)
}

fn collect_ignore_meta(
    meta: &Meta,
    inherited_condition: Option<CfgCondition>,
    annotations: &mut Vec<IgnoreAnnotation>,
) -> Result<(), String> {
    if meta.path().is_ident("ignore") {
        annotations.push(IgnoreAnnotation {
            reason: parse_reasoned_ignore(meta)?,
            guard: inherited_condition,
        });
        return Ok(());
    }

    if !meta.path().is_ident("cfg_attr") {
        return Ok(());
    }
    let Meta::List(list) = meta else {
        return Err("cfg_attr must use parenthesized arguments".to_owned());
    };
    let arguments = parse_meta_list(list)?;
    if arguments.len() < 2 {
        return Err(format!(
            "cfg_attr requires a condition and at least one attribute, found {} argument(s)",
            arguments.len()
        ));
    }
    let local_condition = canonical_cfg_condition(&arguments[0])?;
    let combined_condition = if let Some(inherited) = inherited_condition {
        CfgCondition::all([inherited, local_condition])
    } else {
        local_condition
    };
    for nested_attribute in &arguments[1..] {
        collect_ignore_meta(
            nested_attribute,
            Some(combined_condition.clone()),
            annotations,
        )?;
    }
    Ok(())
}

fn analyze_ignore_attribute(attr: &Attribute) -> Result<Vec<IgnoreAnnotation>, String> {
    let mut annotations = Vec::new();
    collect_ignore_meta(&attr.meta, None, &mut annotations)?;
    Ok(annotations)
}

fn is_ordinary_test_function(function: &syn::ItemFn) -> bool {
    function
        .attrs
        .iter()
        .any(|attr| matches!(&attr.meta, Meta::Path(path) if path.is_ident("test")))
}

struct IgnoreSourceCollector<'a> {
    source_path: &'a str,
    module_path: Vec<String>,
    records: Vec<IgnoredTestSource>,
    allowed_attributes: HashSet<*const Attribute>,
    locators: HashSet<String>,
}

impl IgnoreSourceCollector<'_> {
    fn test_name(&self, function_name: &str) -> String {
        if self.module_path.is_empty() {
            function_name.to_owned()
        } else {
            format!("{}::{function_name}", self.module_path.join("::"))
        }
    }

    fn collect_items(
        &mut self,
        items: &[Item],
        inherited_cfg: Option<CfgCondition>,
    ) -> Result<(), String> {
        for item in items {
            match item {
                Item::Fn(function) => {
                    self.collect_function(function, inherited_cfg.clone())?;
                }
                Item::Mod(module) => {
                    if let Some((_, nested_items)) = &module.content {
                        let module_name = self.test_name(&module.ident.to_string());
                        let module_cfg =
                            effective_cfg_context(inherited_cfg.clone(), &module.attrs).map_err(
                                |error| format!("{}::{module_name}: {error}", self.source_path),
                            )?;
                        self.module_path.push(module.ident.to_string());
                        let result = self.collect_items(nested_items, module_cfg);
                        self.module_path.pop();
                        result?;
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn collect_function(
        &mut self,
        function: &syn::ItemFn,
        inherited_cfg: Option<CfgCondition>,
    ) -> Result<(), String> {
        let test_name = self.test_name(&function.sig.ident.to_string());
        let function_cfg = effective_cfg_context(inherited_cfg, &function.attrs)
            .map_err(|error| format!("{}::{test_name}: {error}", self.source_path))?;
        let mut annotations = Vec::new();
        let mut ignore_attribute_pointers = Vec::new();
        for attr in &function.attrs {
            let parsed = analyze_ignore_attribute(attr)
                .map_err(|error| format!("{}::{test_name}: {error}", self.source_path))?;
            if !parsed.is_empty() {
                ignore_attribute_pointers.push(std::ptr::from_ref(attr));
                annotations.extend(parsed);
            }
        }
        if annotations.is_empty() {
            return Ok(());
        }
        if !is_ordinary_test_function(function) {
            return Err(format!(
                "{}::{test_name}: ignore-bearing attributes must attach to an ordinary `#[test]` function",
                self.source_path
            ));
        }

        let unique: HashSet<_> = annotations.iter().cloned().collect();
        if unique.len() != annotations.len() {
            return Err(format!(
                "{}::{test_name}: duplicate ignore annotation",
                self.source_path
            ));
        }
        if annotations.len() != 1 {
            return Err(format!(
                "{}::{test_name}: {} ignore annotations are overlapping or ambiguous; exactly one is allowed",
                self.source_path,
                annotations.len()
            ));
        }
        let annotation = annotations.pop().expect("one annotation was present");
        let effective_ignore_cfg = conjoin_cfg(function_cfg, annotation.guard);
        let record = IgnoredTestSource {
            source_path: self.source_path.to_owned(),
            test_name,
            reason: annotation.reason,
            cfg_condition: effective_ignore_cfg.map(|condition| condition.render()),
        };
        let locator = record.locator();
        if !self.locators.insert(locator.clone()) {
            return Err(format!("duplicate ignored-test locator `{locator}`"));
        }
        self.allowed_attributes.extend(ignore_attribute_pointers);
        self.records.push(record);
        Ok(())
    }
}

fn scan_tokens_for_ident(input: ParseStream<'_>, expected: &str) -> syn::Result<bool> {
    let mut found = false;
    while !input.is_empty() {
        if input.peek(Lit) {
            let _: Lit = input.parse()?;
            continue;
        }
        if input.peek(syn::Ident) {
            let ident: syn::Ident = input.parse()?;
            if ident == expected {
                found = true;
            }
            continue;
        }
        if input.peek(syn::token::Paren) {
            let nested;
            syn::parenthesized!(nested in input);
            if scan_tokens_for_ident(&nested, expected)? {
                found = true;
            }
            continue;
        }
        if input.peek(syn::token::Bracket) {
            let nested;
            syn::bracketed!(nested in input);
            if scan_tokens_for_ident(&nested, expected)? {
                found = true;
            }
            continue;
        }
        if input.peek(syn::token::Brace) {
            let nested;
            syn::braced!(nested in input);
            if scan_tokens_for_ident(&nested, expected)? {
                found = true;
            }
            continue;
        }
        input.step(|cursor| {
            let Some((_, next)) = cursor.token_tree() else {
                return Err(cursor.error("expected a macro token"));
            };
            Ok(((), next))
        })?;
    }
    Ok(found)
}

fn scan_tokens_for_dollar(input: ParseStream<'_>) -> syn::Result<bool> {
    let mut found = false;
    while !input.is_empty() {
        if input.peek(Lit) {
            let _: Lit = input.parse()?;
            continue;
        }
        if input.peek(Token![$]) {
            let _: Token![$] = input.parse()?;
            found = true;
            continue;
        }
        if input.peek(syn::token::Paren) {
            let nested;
            syn::parenthesized!(nested in input);
            if scan_tokens_for_dollar(&nested)? {
                found = true;
            }
            continue;
        }
        if input.peek(syn::token::Bracket) {
            let nested;
            syn::bracketed!(nested in input);
            if scan_tokens_for_dollar(&nested)? {
                found = true;
            }
            continue;
        }
        if input.peek(syn::token::Brace) {
            let nested;
            syn::braced!(nested in input);
            if scan_tokens_for_dollar(&nested)? {
                found = true;
            }
            continue;
        }
        input.step(|cursor| {
            let Some((_, next)) = cursor.token_tree() else {
                return Err(cursor.error("expected a macro token"));
            };
            Ok(((), next))
        })?;
    }
    Ok(found)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MacroFinding {
    TestAttribute,
    IgnoreAttribute,
    MetavariableAttribute,
    DynamicMacroCallee,
}

impl MacroFinding {
    const fn description(self) -> &'static str {
        match self {
            Self::TestAttribute => "a test attribute",
            Self::IgnoreAttribute => "an ignore-bearing attribute",
            Self::MetavariableAttribute => "a macro-forwarded attribute",
            Self::DynamicMacroCallee => "a dynamically selected macro invocation",
        }
    }

    const fn priority(self) -> u8 {
        match self {
            Self::TestAttribute => 1,
            Self::IgnoreAttribute => 2,
            Self::MetavariableAttribute | Self::DynamicMacroCallee => 3,
        }
    }
}

fn record_macro_finding(finding: &mut Option<MacroFinding>, candidate: MacroFinding) {
    if finding.is_none_or(|current| candidate.priority() > current.priority()) {
        *finding = Some(candidate);
    }
}

fn meta_macro_finding(meta: &Meta) -> Option<MacroFinding> {
    if meta.path().is_ident("test") {
        return Some(MacroFinding::TestAttribute);
    }
    if meta.path().is_ident("ignore") {
        return Some(MacroFinding::IgnoreAttribute);
    }
    let Meta::List(list) = meta else {
        return None;
    };
    if !list.path.is_ident("cfg_attr") {
        return None;
    }
    let Ok(arguments) = parse_meta_list(list) else {
        return Some(MacroFinding::MetavariableAttribute);
    };
    let mut finding = None;
    for argument in arguments.iter().skip(1) {
        if let Some(candidate) = meta_macro_finding(argument) {
            record_macro_finding(&mut finding, candidate);
        }
    }
    finding
}

fn macro_attribute_at_cursor_finding(input: ParseStream<'_>) -> syn::Result<Option<MacroFinding>> {
    let fork = input.fork();
    if !fork.peek(Token![#]) {
        return Ok(None);
    }
    let _: Token![#] = fork.parse()?;
    if fork.peek(Token![!]) {
        let _: Token![!] = fork.parse()?;
    }
    if !fork.peek(syn::token::Bracket) {
        return Ok(None);
    }
    let content;
    syn::bracketed!(content in fork);
    let meta_input = content.fork();
    if let Ok(meta) = meta_input.parse::<Meta>() {
        if meta_input.is_empty() {
            return Ok(meta_macro_finding(&meta));
        }
    }

    if scan_tokens_for_dollar(&content.fork())? {
        return Ok(Some(MacroFinding::MetavariableAttribute));
    }
    if scan_tokens_for_ident(&content.fork(), "ignore")? {
        return Ok(Some(MacroFinding::IgnoreAttribute));
    }
    if scan_tokens_for_ident(&content.fork(), "test")? {
        return Ok(Some(MacroFinding::TestAttribute));
    }
    Ok(None)
}

fn dynamic_macro_callee_at_cursor(input: ParseStream<'_>) -> syn::Result<bool> {
    let fork = input.fork();
    if !fork.peek(Token![$]) {
        return Ok(false);
    }
    let _: Token![$] = fork.parse()?;
    if !fork.peek(syn::Ident) {
        return Ok(false);
    }
    let _: syn::Ident = fork.parse()?;
    Ok(fork.peek(Token![!]))
}

fn validate_literal_include(
    input: ParseStream<'_>,
    source_path: &str,
    inventory_universe: &HashSet<String>,
) -> syn::Result<()> {
    let literal: syn::LitStr = input.parse().map_err(|error| {
        syn::Error::new(
            error.span(),
            "include! must use one tracked repository-relative Rust string literal",
        )
    })?;
    if input.peek(Token![,]) {
        let _: Token![,] = input.parse()?;
    }
    if !input.is_empty() {
        return Err(input.error("include! accepts exactly one string literal"));
    }

    let source_parent = Path::new(source_path)
        .parent()
        .unwrap_or_else(|| Path::new(""));
    let include_path = source_parent.join(literal.value());
    let normalized = normalize_source_path(&include_path)
        .map_err(|error| syn::Error::new(literal.span(), error))?;
    if Path::new(&normalized)
        .extension()
        .and_then(|extension| extension.to_str())
        != Some("rs")
    {
        return Err(syn::Error::new(
            literal.span(),
            format!("include! target must be a `.rs` file, found `{normalized}`"),
        ));
    }
    if !inventory_universe.contains(&normalized) {
        return Err(syn::Error::new(
            literal.span(),
            format!("include! target is not tracked by the source inventory: `{normalized}`"),
        ));
    }
    Ok(())
}

fn static_include_at_cursor(
    input: ParseStream<'_>,
    source_path: &str,
    inventory_universe: &HashSet<String>,
) -> syn::Result<bool> {
    let fork = input.fork();
    if !fork.peek(syn::Ident) {
        return Ok(false);
    }
    let macro_name: syn::Ident = fork.parse()?;
    if macro_name != "include" || !fork.peek(Token![!]) {
        return Ok(false);
    }
    let _: Token![!] = fork.parse()?;
    if fork.peek(syn::token::Paren) {
        let nested;
        syn::parenthesized!(nested in fork);
        validate_literal_include(&nested, source_path, inventory_universe)?;
    } else if fork.peek(syn::token::Bracket) {
        let nested;
        syn::bracketed!(nested in fork);
        validate_literal_include(&nested, source_path, inventory_universe)?;
    } else if fork.peek(syn::token::Brace) {
        let nested;
        syn::braced!(nested in fork);
        validate_literal_include(&nested, source_path, inventory_universe)?;
    } else {
        return Err(fork.error("include! must use a delimited argument"));
    }
    Ok(true)
}

fn scan_macro_tokens(
    input: ParseStream<'_>,
    source_path: &str,
    inventory_universe: &HashSet<String>,
) -> syn::Result<Option<MacroFinding>> {
    let mut finding = None;
    while !input.is_empty() {
        if let Some(candidate) = macro_attribute_at_cursor_finding(input)? {
            record_macro_finding(&mut finding, candidate);
        }
        static_include_at_cursor(input, source_path, inventory_universe)?;
        if dynamic_macro_callee_at_cursor(input)? {
            record_macro_finding(&mut finding, MacroFinding::DynamicMacroCallee);
        }
        if input.peek(Lit) {
            let _: Lit = input.parse()?;
            continue;
        }
        if input.peek(syn::token::Paren) {
            let nested;
            syn::parenthesized!(nested in input);
            if let Some(nested_finding) =
                scan_macro_tokens(&nested, source_path, inventory_universe)?
            {
                record_macro_finding(&mut finding, nested_finding);
            }
            continue;
        }
        if input.peek(syn::token::Bracket) {
            let nested;
            syn::bracketed!(nested in input);
            if let Some(nested_finding) =
                scan_macro_tokens(&nested, source_path, inventory_universe)?
            {
                record_macro_finding(&mut finding, nested_finding);
            }
            continue;
        }
        if input.peek(syn::token::Brace) {
            let nested;
            syn::braced!(nested in input);
            if let Some(nested_finding) =
                scan_macro_tokens(&nested, source_path, inventory_universe)?
            {
                record_macro_finding(&mut finding, nested_finding);
            }
            continue;
        }
        input.step(|cursor| {
            let Some((_, next)) = cursor.token_tree() else {
                return Err(cursor.error("expected a macro token"));
            };
            Ok(((), next))
        })?;
    }
    Ok(finding)
}

struct IgnorePlacementAudit<'a> {
    source_path: &'a str,
    inventory_universe: &'a HashSet<String>,
    allowed_attributes: &'a HashSet<*const Attribute>,
    error: Option<String>,
}

fn is_audited_proptest_macro(path: &syn::Path) -> bool {
    if path.is_ident("proptest") {
        return true;
    }
    if path.leading_colon.is_some() {
        return false;
    }
    let mut segments = path.segments.iter();
    matches!(
        (segments.next(), segments.next(), segments.next()),
        (Some(first), Some(second), None)
            if first.ident == "proptest" && second.ident == "proptest"
    )
}

impl<'ast> Visit<'ast> for IgnorePlacementAudit<'_> {
    fn visit_attribute(&mut self, attr: &'ast Attribute) {
        if self.error.is_some() || self.allowed_attributes.contains(&std::ptr::from_ref(attr)) {
            return;
        }
        match analyze_ignore_attribute(attr) {
            Ok(annotations) if !annotations.is_empty() => {
                self.error = Some(format!(
                    "{}: ignore-bearing attributes must attach to an ordinary `#[test]` function",
                    self.source_path
                ));
            }
            Err(error) => {
                self.error = Some(format!("{}: {error}", self.source_path));
            }
            Ok(_) => {}
        }
    }

    fn visit_macro(&mut self, item_macro: &'ast syn::Macro) {
        if self.error.is_some() {
            return;
        }
        let macro_name = item_macro
            .path
            .segments
            .iter()
            .map(|segment| segment.ident.to_string())
            .collect::<Vec<_>>()
            .join("::");
        if item_macro
            .path
            .segments
            .last()
            .is_some_and(|segment| segment.ident == "include")
        {
            let parser = |input: ParseStream<'_>| {
                validate_literal_include(input, self.source_path, self.inventory_universe)
            };
            if let Err(error) = parser.parse2(item_macro.tokens.clone()) {
                self.error = Some(format!(
                    "{}: invalid include! source boundary: {error}",
                    self.source_path
                ));
            }
            return;
        }

        let parser = |input: ParseStream<'_>| {
            scan_macro_tokens(input, self.source_path, self.inventory_universe)
        };
        match parser.parse2(item_macro.tokens.clone()) {
            Ok(Some(MacroFinding::TestAttribute))
                if is_audited_proptest_macro(&item_macro.path) => {}
            Ok(Some(finding)) => {
                self.error = Some(format!(
                    "{}: macro `{macro_name}!` contains {} that cannot be audited without expansion",
                    self.source_path,
                    finding.description()
                ));
            }
            Ok(None) => {}
            Err(error) => {
                self.error = Some(format!(
                    "{}: unable to audit macro token tree at {:?}: {error}",
                    self.source_path,
                    item_macro.span()
                ));
            }
        }
    }
}

fn collect_ignored_tests(path: &Path, source: &str) -> Result<Vec<IgnoredTestSource>, String> {
    let source_path = normalize_source_path(path)?;
    let inventory_universe = HashSet::from([source_path]);
    collect_ignored_tests_with_inventory(path, source, &inventory_universe)
}

fn collect_ignored_tests_with_inventory(
    path: &Path,
    source: &str,
    inventory_universe: &HashSet<String>,
) -> Result<Vec<IgnoredTestSource>, String> {
    let source_path = normalize_source_path(path)?;
    if !inventory_universe.contains(&source_path) {
        return Err(format!(
            "source path is absent from the tracked inventory universe: `{source_path}`"
        ));
    }
    let syntax = syn::parse_file(source)
        .map_err(|error| format!("{source_path}: unable to parse Rust source: {error}"))?;
    let mut collector = IgnoreSourceCollector {
        source_path: &source_path,
        module_path: Vec::new(),
        records: Vec::new(),
        allowed_attributes: HashSet::new(),
        locators: HashSet::new(),
    };
    let file_cfg = cfg_gate_from_attributes(&syntax.attrs)
        .map_err(|error| format!("{source_path}: {error}"))?;
    collector.collect_items(&syntax.items, file_cfg)?;

    let mut audit = IgnorePlacementAudit {
        source_path: &source_path,
        inventory_universe,
        allowed_attributes: &collector.allowed_attributes,
        error: None,
    };
    audit.visit_file(&syntax);
    if let Some(error) = audit.error {
        return Err(error);
    }

    collector.records.sort_by_key(IgnoredTestSource::locator);
    Ok(collector.records)
}

fn tracked_rust_source_paths(root: &Path) -> Result<Vec<String>, String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(["ls-files", "-z", "--", "*.rs"])
        .output()
        .map_err(|error| format!("unable to enumerate tracked Rust sources: {error}"))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!(
            "tracked Rust source enumeration failed with {}: {}",
            output.status,
            stderr.trim()
        ));
    }

    let mut paths = Vec::new();
    let mut unique = HashSet::new();
    for raw_path in output.stdout.split(|byte| *byte == 0) {
        if raw_path.is_empty() {
            continue;
        }
        let raw_path = std::str::from_utf8(raw_path)
            .map_err(|error| format!("tracked Rust source path is not valid UTF-8: {error}"))?;
        let normalized = normalize_source_path(Path::new(raw_path))?;
        if normalized != raw_path {
            return Err(format!(
                "tracked Rust source path is not canonically spelled: `{raw_path}` -> `{normalized}`"
            ));
        }
        if !unique.insert(normalized.clone()) {
            return Err(format!(
                "tracked Rust source enumeration returned duplicate path `{normalized}`"
            ));
        }

        let metadata = fs::symlink_metadata(root.join(&normalized)).map_err(|error| {
            format!("unable to inspect tracked Rust source `{normalized}`: {error}")
        })?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(format!(
                "tracked Rust source must be a regular non-symlink file: `{normalized}`"
            ));
        }
        paths.push(normalized);
    }
    if paths.is_empty() {
        return Err("tracked Rust source inventory is empty".to_owned());
    }
    paths.sort_unstable();
    Ok(paths)
}

fn collect_repository_ignored_tests_from_paths_with_reader<F>(
    source_paths: &[String],
    uninspected_source_paths: &[&str],
    mut read_source: F,
) -> Result<RepositoryIgnoreInventory, String>
where
    F: FnMut(&str) -> Result<String, String>,
{
    let mut ordered_source_paths = source_paths.to_vec();
    ordered_source_paths.sort_unstable();
    let mut inventory_universe = HashSet::new();
    for source_path in &ordered_source_paths {
        let normalized = normalize_source_path(Path::new(source_path))?;
        if normalized != *source_path {
            return Err(format!(
                "tracked Rust source path is not canonically spelled: `{source_path}` -> `{normalized}`"
            ));
        }
        if Path::new(source_path)
            .extension()
            .and_then(|extension| extension.to_str())
            != Some("rs")
        {
            return Err(format!(
                "tracked Rust source must identify a `.rs` file: `{source_path}`"
            ));
        }
        if !inventory_universe.insert(source_path.clone()) {
            return Err(format!(
                "tracked Rust source inventory contains duplicate path `{source_path}`"
            ));
        }
    }
    if inventory_universe.is_empty() {
        return Err("tracked Rust source inventory is empty".to_owned());
    }

    let mut uninspected = HashSet::new();
    for source_path in uninspected_source_paths {
        let normalized = normalize_source_path(Path::new(source_path))?;
        if normalized != *source_path {
            return Err(format!(
                "uninspected Rust source path is not canonically spelled: `{source_path}` -> `{normalized}`"
            ));
        }
        if !inventory_universe.contains(&normalized) {
            return Err(format!(
                "uninspected Rust source is not tracked: `{normalized}`"
            ));
        }
        if !uninspected.insert(normalized.clone()) {
            return Err(format!(
                "duplicate uninspected Rust source path: `{normalized}`"
            ));
        }
    }

    let mut records = Vec::new();
    let mut locators = HashSet::new();
    let mut encountered_uninspected = Vec::new();

    for source_path in ordered_source_paths {
        if uninspected.contains(&source_path) {
            encountered_uninspected.push(source_path);
            continue;
        }
        let source = read_source(&source_path)?;
        for record in collect_ignored_tests_with_inventory(
            Path::new(&source_path),
            &source,
            &inventory_universe,
        )? {
            let locator = record.locator();
            if !locators.insert(locator.clone()) {
                return Err(format!(
                    "tracked Rust source inventory contains duplicate ignored-test locator `{locator}`"
                ));
            }
            records.push(record);
        }
    }

    records.sort_by_key(IgnoredTestSource::locator);
    Ok(RepositoryIgnoreInventory {
        records,
        uninspected_sources: encountered_uninspected,
        soundness_limitations: Vec::new(),
    })
}

fn collect_repository_ignored_tests(
    root: &Path,
    uninspected_source_paths: &[&str],
) -> Result<RepositoryIgnoreInventory, String> {
    let source_paths = tracked_rust_source_paths(root)?;
    let mut snapshots = Vec::new();
    let mut inventory = collect_repository_ignored_tests_from_paths_with_reader(
        &source_paths,
        uninspected_source_paths,
        |source_path| {
            let bytes = fs::read(root.join(source_path)).map_err(|error| {
                format!("unable to read tracked Rust source `{source_path}`: {error}")
            })?;
            let source = String::from_utf8(bytes).map_err(|error| {
                format!("tracked Rust source `{source_path}` is not valid UTF-8: {error}")
            })?;
            snapshots.push((source_path.to_owned(), blake3::hash(source.as_bytes())));
            Ok(source)
        },
    )?;

    for (source_path, expected_hash) in snapshots {
        let metadata = fs::symlink_metadata(root.join(&source_path)).map_err(|error| {
            format!("unable to re-inspect tracked Rust source `{source_path}`: {error}")
        })?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(format!(
                "tracked Rust source changed file type during inventory: `{source_path}`"
            ));
        }
        let current = fs::read(root.join(&source_path)).map_err(|error| {
            format!("unable to re-read tracked Rust source `{source_path}`: {error}")
        })?;
        if blake3::hash(&current) != expected_hash {
            return Err(format!(
                "tracked Rust source changed during inventory; retry from a stable snapshot: `{source_path}`"
            ));
        }
    }

    inventory.soundness_limitations = SOURCE_INVENTORY_SOUNDNESS_LIMITATIONS
        .iter()
        .map(|limitation| (*limitation).to_owned())
        .collect();

    Ok(inventory)
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum IgnoreKind {
    KnownBug,
    Placeholder,
    Performance,
    Stress,
    Diagnostic,
    SubprocessHelper,
    ArtifactGeneration,
    EnvironmentSpecific,
    ReleaseGate,
}

impl IgnoreKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::KnownBug => "known_bug",
            Self::Placeholder => "placeholder",
            Self::Performance => "performance",
            Self::Stress => "stress",
            Self::Diagnostic => "diagnostic",
            Self::SubprocessHelper => "subprocess_helper",
            Self::ArtifactGeneration => "artifact_generation",
            Self::EnvironmentSpecific => "environment_specific",
            Self::ReleaseGate => "release_gate",
        }
    }

    const fn allows_policy(self, policy: IgnorePolicy) -> bool {
        match self {
            Self::KnownBug | Self::Placeholder => matches!(policy, IgnorePolicy::BlockRelease),
            Self::Performance => {
                matches!(policy, IgnorePolicy::RunForRelease | IgnorePolicy::Exempt)
            }
            Self::Stress => matches!(policy, IgnorePolicy::RunForRelease),
            Self::Diagnostic | Self::ArtifactGeneration | Self::EnvironmentSpecific => {
                matches!(policy, IgnorePolicy::Exempt)
            }
            Self::SubprocessHelper => matches!(policy, IgnorePolicy::CoveredByParent),
            Self::ReleaseGate => {
                matches!(
                    policy,
                    IgnorePolicy::BlockRelease | IgnorePolicy::RunForRelease
                )
            }
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum IgnorePolicy {
    BlockRelease,
    RunForRelease,
    CoveredByParent,
    Exempt,
}

impl IgnorePolicy {
    const fn as_str(self) -> &'static str {
        match self {
            Self::BlockRelease => "block_release",
            Self::RunForRelease => "run_for_release",
            Self::CoveredByParent => "covered_by_parent",
            Self::Exempt => "exempt",
        }
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct IgnoreEvidence {
    requirement: String,
    receipt: Option<IgnoreEvidenceReceipt>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct IgnoreEvidenceReceipt {
    source_commit: String,
    artifact_path: String,
    artifact_blake3: String,
    parent_source_path: Option<String>,
    parent_test_name: Option<String>,
}

fn is_lowercase_hex(value: &str) -> bool {
    value
        .bytes()
        .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn validate_source_test_identity(source_path: &str, test_name: &str) -> Result<(), String> {
    let normalized = normalize_source_path(Path::new(source_path))?;
    if normalized != source_path {
        return Err(format!(
            "source_path is not canonically spelled: `{source_path}` -> `{normalized}`"
        ));
    }
    if Path::new(source_path)
        .extension()
        .and_then(|extension| extension.to_str())
        != Some("rs")
    {
        return Err("source_path must identify a `.rs` file".to_owned());
    }
    if test_name.trim() != test_name
        || test_name
            .split("::")
            .any(|segment| segment.is_empty() || syn::parse_str::<syn::Ident>(segment).is_err())
    {
        return Err(format!(
            "test_name must be a canonical Rust identifier path, found {test_name:?}"
        ));
    }
    Ok(())
}

impl IgnoreEvidenceReceipt {
    fn validate(&self) -> Result<(), String> {
        if self.source_commit.len() != 40 || !is_lowercase_hex(&self.source_commit) {
            return Err(
                "receipt.source_commit must be a 40-digit lowercase hexadecimal commit".to_owned(),
            );
        }
        if self.artifact_blake3.len() != 64 || !is_lowercase_hex(&self.artifact_blake3) {
            return Err(
                "receipt.artifact_blake3 must be a 64-digit lowercase hexadecimal digest"
                    .to_owned(),
            );
        }
        let normalized_artifact = normalize_source_path(Path::new(&self.artifact_path))?;
        if normalized_artifact != self.artifact_path {
            return Err(format!(
                "receipt.artifact_path is not canonically spelled: `{}` -> `{normalized_artifact}`",
                self.artifact_path
            ));
        }
        match (&self.parent_source_path, &self.parent_test_name) {
            (Some(source_path), Some(test_name)) => {
                validate_source_test_identity(source_path, test_name)
                    .map_err(|error| format!("invalid receipt parent identity: {error}"))?;
            }
            (None, None) => {}
            _ => {
                return Err(
                    "receipt parent_source_path and parent_test_name must both be null or both be present"
                        .to_owned(),
                );
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct IgnoredTestBaseline {
    source_path: String,
    test_name: String,
    reason: String,
    cfg_condition: Option<String>,
    kind: IgnoreKind,
    policy: IgnorePolicy,
    evidence: IgnoreEvidence,
}

impl IgnoredTestBaseline {
    fn locator(&self) -> String {
        format!("{}::{}", self.source_path, self.test_name)
    }

    fn validate(&self) -> Result<(), String> {
        if !self.kind.allows_policy(self.policy) {
            return Err(format!(
                "kind `{}` cannot use policy `{}`",
                self.kind.as_str(),
                self.policy.as_str()
            ));
        }
        validate_source_test_identity(&self.source_path, &self.test_name)?;
        if self.reason.trim().is_empty() || self.reason.trim() != self.reason {
            return Err("reason must be nonempty and have no surrounding whitespace".to_owned());
        }
        if let Some(condition) = &self.cfg_condition {
            let meta = syn::parse_str::<Meta>(condition).map_err(|error| {
                format!("cfg_condition is not valid cfg syntax `{condition}`: {error}")
            })?;
            let canonical = canonical_cfg_condition(&meta)?.render();
            if canonical != *condition {
                return Err(format!(
                    "cfg_condition is not canonical: `{condition}` -> `{canonical}`"
                ));
            }
        }
        if self.evidence.requirement.trim().is_empty()
            || self.evidence.requirement.trim() != self.evidence.requirement
        {
            return Err(
                "evidence.requirement must be nonempty and have no surrounding whitespace"
                    .to_owned(),
            );
        }
        if let Some(receipt) = &self.evidence.receipt {
            receipt.validate()?;
            let has_parent = receipt.parent_source_path.is_some();
            if self.policy == IgnorePolicy::CoveredByParent && !has_parent {
                return Err("covered_by_parent receipt must name its parent test".to_owned());
            }
            if self.policy != IgnorePolicy::CoveredByParent && has_parent {
                return Err("only covered_by_parent receipts may name a parent test".to_owned());
            }
            if has_parent
                && receipt.parent_source_path.as_deref() == Some(self.source_path.as_str())
                && receipt.parent_test_name.as_deref() == Some(self.test_name.as_str())
            {
                return Err("covered_by_parent receipt cannot name itself as parent".to_owned());
            }
        }
        Ok(())
    }

    fn source_identity(&self) -> IgnoredTestSource {
        IgnoredTestSource {
            source_path: self.source_path.clone(),
            test_name: self.test_name.clone(),
            reason: self.reason.clone(),
            cfg_condition: self.cfg_condition.clone(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct RegressionBaseline {
    as_of_phase: String,
    total_tests: u64,
    passed: u64,
    failed: u64,
    ignored: u64,
    baseline_commit: String,
    ignored_tests: Vec<IgnoredTestBaseline>,
}

impl RegressionBaseline {
    fn validate(&self) -> Result<(), String> {
        if self.as_of_phase.trim().is_empty() {
            return Err("as_of_phase must not be empty".to_owned());
        }
        if self.total_tests == 0 {
            return Err("total_tests must be greater than zero".to_owned());
        }

        let accounted_tests = self
            .passed
            .checked_add(self.failed)
            .and_then(|count| count.checked_add(self.ignored))
            .ok_or_else(|| "baseline test counts overflowed u64".to_owned())?;
        if self.total_tests != accounted_tests {
            return Err(format!(
                "total_tests={} does not equal passed + failed + ignored={accounted_tests}",
                self.total_tests
            ));
        }
        if self.failed != 0 {
            return Err(format!(
                "release regression baseline must have zero failures, found {}",
                self.failed
            ));
        }
        let commit = &self.baseline_commit;
        if !(7..=40).contains(&commit.len()) || !is_lowercase_hex(commit) {
            return Err(format!(
                "baseline_commit must be an untrimmed 7-40 digit lowercase hexadecimal Git object name, found `{commit}`"
            ));
        }

        let mut previous_locator = None;
        for entry in &self.ignored_tests {
            let locator = entry.locator();
            entry
                .validate()
                .map_err(|error| format!("ignored_tests entry `{locator}` is invalid: {error}"))?;
            if previous_locator
                .as_ref()
                .is_some_and(|previous| previous >= &locator)
            {
                return Err(format!(
                    "ignored_tests locators must be strictly sorted and unique: `{locator}` follows `{}`",
                    previous_locator.as_deref().unwrap_or_default()
                ));
            }
            previous_locator = Some(locator);
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RegressionCounts {
    total_tests: u64,
    passed: u64,
    failed: u64,
    ignored: u64,
}

impl RegressionCounts {
    const fn zero() -> Self {
        Self {
            total_tests: 0,
            passed: 0,
            failed: 0,
            ignored: 0,
        }
    }

    fn checked_add(&mut self, rhs: Self) -> Result<(), String> {
        let total_tests = self
            .total_tests
            .checked_add(rhs.total_tests)
            .ok_or_else(|| "aggregate total_tests overflowed u64".to_owned())?;
        let passed = self
            .passed
            .checked_add(rhs.passed)
            .ok_or_else(|| "aggregate passed count overflowed u64".to_owned())?;
        let failed = self
            .failed
            .checked_add(rhs.failed)
            .ok_or_else(|| "aggregate failed count overflowed u64".to_owned())?;
        let ignored = self
            .ignored
            .checked_add(rhs.ignored)
            .ok_or_else(|| "aggregate ignored count overflowed u64".to_owned())?;

        *self = Self {
            total_tests,
            passed,
            failed,
            ignored,
        };
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct CommandEvidence {
    argv: Vec<String>,
    exit_status: i32,
    capture_status: i32,
    artifact_path: String,
    artifact_blake3: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct CurrentRunReceipt {
    source_path: String,
    test_name: String,
    requirement_blake3: String,
    execution: CommandEvidence,
}

impl CurrentRunReceipt {
    fn locator(&self) -> String {
        format!("{}::{}", self.source_path, self.test_name)
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct ReleaseEvidenceManifest {
    schema_version: u32,
    tested_commit: String,
    workspace: CommandEvidence,
    run_receipts: Vec<CurrentRunReceipt>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct ValidatedCurrentRunReceipts {
    locators: HashSet<String>,
}

impl ValidatedCurrentRunReceipts {
    fn contains(&self, locator: &str) -> bool {
        self.locators.contains(locator)
    }

    #[cfg(test)]
    fn from_test_locators(locators: impl IntoIterator<Item = String>) -> Self {
        Self {
            locators: locators.into_iter().collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ValidatedReleaseEvidence {
    tested_commit: String,
    workspace_transcript: String,
    workspace_counts: RegressionCounts,
    current_run_receipts: ValidatedCurrentRunReceipts,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CargoTestTarget {
    Library,
    Integration(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RegressionDelta {
    delta_total: i64,
    delta_passed: i64,
    delta_failed: i64,
    delta_ignored: i64,
    new_tests: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RegressionReport {
    pass: bool,
    delta: RegressionDelta,
    reason: Option<String>,
}

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("harness crate should be nested under workspace root")
}

fn baseline_path(root: &Path) -> PathBuf {
    root.join(REGRESSION_BASELINE_PATH)
}

fn canonical_evidence_path(path: &str, description: &str) -> Result<String, String> {
    let normalized = normalize_source_path(Path::new(path))?;
    if normalized != path {
        return Err(format!(
            "{description} is not canonically spelled: `{path}` -> `{normalized}`"
        ));
    }
    if !path.starts_with(EVIDENCE_PATH_PREFIX) {
        return Err(format!(
            "{description} must be stored below `{EVIDENCE_PATH_PREFIX}`: `{path}`"
        ));
    }
    Ok(normalized)
}

fn validate_command_evidence_shape(
    evidence: &CommandEvidence,
    description: &str,
) -> Result<(), String> {
    if evidence.argv.is_empty()
        || evidence
            .argv
            .iter()
            .any(|argument| argument.is_empty() || argument.contains('\0'))
    {
        return Err(format!(
            "{description}.argv must contain nonempty, NUL-free argument tokens"
        ));
    }
    if evidence.exit_status != 0 || evidence.capture_status != 0 {
        return Err(format!(
            "{description} must record zero command and capture statuses"
        ));
    }
    canonical_evidence_path(
        &evidence.artifact_path,
        &format!("{description}.artifact_path"),
    )?;
    if evidence.artifact_blake3.len() != 64 || !is_lowercase_hex(&evidence.artifact_blake3) {
        return Err(format!(
            "{description}.artifact_blake3 must be a 64-digit lowercase hexadecimal digest"
        ));
    }
    Ok(())
}

fn expected_current_run_target(entry: &IgnoredTestBaseline) -> Result<CargoTestTarget, String> {
    let components = entry.source_path.split('/').collect::<Vec<_>>();
    if components.len() < 4 || components[0] != "crates" {
        return Err(format!(
            "run_for_release source is outside a supported workspace crate target: `{}`",
            entry.source_path
        ));
    }
    match components[2] {
        "src" if components[3] != "main.rs" && components[3] != "bin" => {
            Ok(CargoTestTarget::Library)
        }
        "tests" if components.len() == 4 => {
            let target = Path::new(components[3])
                .file_stem()
                .and_then(|stem| stem.to_str())
                .ok_or_else(|| {
                    format!(
                        "run_for_release integration target has no UTF-8 file stem: `{}`",
                        entry.source_path
                    )
                })?;
            Ok(CargoTestTarget::Integration(target.to_owned()))
        }
        _ => Err(format!(
            "run_for_release source is not an unambiguous library or integration-test target: `{}`",
            entry.source_path
        )),
    }
}

fn expected_current_run_argv(entry: &IgnoredTestBaseline) -> Result<Vec<String>, String> {
    let package = entry.source_path.split('/').nth(1).ok_or_else(|| {
        format!(
            "run_for_release source has no crate directory: `{}`",
            entry.source_path
        )
    })?;
    let target = expected_current_run_target(entry)?;
    let needs_ignored_filter = match entry.cfg_condition.as_deref() {
        None | Some("test") => true,
        Some("debug_assertions" | "all(debug_assertions,test)") => false,
        Some(condition) => {
            return Err(format!(
                "run_for_release ignore condition `{condition}` requires a typed compilation contract"
            ));
        }
    };
    let mut argv = vec![
        "cargo".to_owned(),
        "test".to_owned(),
        "--locked".to_owned(),
        "--profile".to_owned(),
        "release-perf".to_owned(),
        "--package".to_owned(),
        package.to_owned(),
    ];
    match target {
        CargoTestTarget::Library => argv.push("--lib".to_owned()),
        CargoTestTarget::Integration(target) => {
            argv.push("--test".to_owned());
            argv.push(target);
        }
    }
    argv.extend([
        entry.test_name.clone(),
        "--".to_owned(),
        "--exact".to_owned(),
    ]);
    if needs_ignored_filter {
        argv.push("--ignored".to_owned());
    }
    argv.push("--nocapture".to_owned());
    argv.push("--test-threads=1".to_owned());
    Ok(argv)
}

fn validate_cargo_manifest_target_contract(
    manifest: &toml::Table,
    crate_directory: &str,
    entry: &IgnoredTestBaseline,
) -> Result<(), String> {
    let package = manifest
        .get("package")
        .and_then(toml::Value::as_table)
        .ok_or_else(|| "crate manifest has no [package] table".to_owned())?;
    let package_name = package
        .get("name")
        .and_then(toml::Value::as_str)
        .ok_or_else(|| "crate manifest has no package.name".to_owned())?;
    if package_name != crate_directory {
        return Err(format!(
            "run_for_release crate directory `{crate_directory}` does not equal package.name `{package_name}`; a typed target contract is required"
        ));
    }

    match expected_current_run_target(entry)? {
        CargoTestTarget::Library => {
            if package
                .get("autolib")
                .and_then(toml::Value::as_bool)
                .is_some_and(|enabled| !enabled)
            {
                return Err(
                    "run_for_release library target requires Cargo auto-discovery".to_owned(),
                );
            }
            if let Some(library) = manifest.get("lib").and_then(toml::Value::as_table) {
                if library
                    .get("path")
                    .and_then(toml::Value::as_str)
                    .is_some_and(|path| path != "src/lib.rs")
                    || library
                        .get("test")
                        .and_then(toml::Value::as_bool)
                        .is_some_and(|enabled| !enabled)
                    || library
                        .get("harness")
                        .and_then(toml::Value::as_bool)
                        .is_some_and(|enabled| !enabled)
                    || library.contains_key("required-features")
                {
                    return Err(
                        "run_for_release library target has a noncanonical Cargo override"
                            .to_owned(),
                    );
                }
            }
        }
        CargoTestTarget::Integration(target) => {
            if package
                .get("autotests")
                .and_then(toml::Value::as_bool)
                .is_some_and(|enabled| !enabled)
            {
                return Err(
                    "run_for_release integration target requires Cargo auto-discovery".to_owned(),
                );
            }
            let relative_source = entry
                .source_path
                .strip_prefix(&format!("crates/{crate_directory}/"))
                .expect("source identity already established the crate directory");
            if let Some(tests) = manifest.get("test").and_then(toml::Value::as_array) {
                for test in tests.iter().filter_map(toml::Value::as_table) {
                    let same_name =
                        test.get("name").and_then(toml::Value::as_str) == Some(target.as_str());
                    let same_path = test
                        .get("path")
                        .and_then(toml::Value::as_str)
                        .map(|path| normalize_source_path(Path::new(path)))
                        .transpose()?
                        .as_deref()
                        == Some(relative_source);
                    if same_name || same_path {
                        return Err(
                            "run_for_release integration target has an explicit Cargo override"
                                .to_owned(),
                        );
                    }
                }
            }
        }
    }
    Ok(())
}

fn validate_cargo_target_mapping(root: &Path, entry: &IgnoredTestBaseline) -> Result<(), String> {
    let crate_directory = entry.source_path.split('/').nth(1).ok_or_else(|| {
        format!(
            "run_for_release source has no crate directory: `{}`",
            entry.source_path
        )
    })?;
    let manifest_path = format!("crates/{crate_directory}/Cargo.toml");
    require_clean_tracked_path(root, &manifest_path, "run_for_release crate manifest")?;
    require_regular_non_symlink_path(root, &manifest_path, "run_for_release crate manifest")?;
    let manifest = fs::read_to_string(root.join(&manifest_path)).map_err(|error| {
        format!("unable to read run_for_release crate manifest `{manifest_path}`: {error}")
    })?;
    let manifest = toml::from_str::<toml::Table>(&manifest).map_err(|error| {
        format!("unable to parse run_for_release crate manifest `{manifest_path}`: {error}")
    })?;
    validate_cargo_manifest_target_contract(&manifest, crate_directory, entry)
        .map_err(|error| format!("invalid target mapping in `{manifest_path}`: {error}"))?;
    if expected_current_run_target(entry)? == CargoTestTarget::Library {
        let library_root = format!("crates/{crate_directory}/src/lib.rs");
        require_clean_tracked_path(root, &library_root, "run_for_release library root")?;
        require_regular_non_symlink_path(root, &library_root, "run_for_release library root")?;
    }
    Ok(())
}

fn validate_release_evidence_manifest(
    manifest: &ReleaseEvidenceManifest,
    baseline: &RegressionBaseline,
) -> Result<ValidatedCurrentRunReceipts, String> {
    baseline
        .validate()
        .map_err(|error| format!("release evidence baseline is invalid: {error}"))?;
    if manifest.schema_version != EVIDENCE_SCHEMA_VERSION {
        return Err(format!(
            "unsupported release evidence schema version {}; expected {EVIDENCE_SCHEMA_VERSION}",
            manifest.schema_version
        ));
    }
    if manifest.tested_commit.len() != 40 || !is_lowercase_hex(&manifest.tested_commit) {
        return Err(
            "release evidence tested_commit must be a 40-digit lowercase hexadecimal commit"
                .to_owned(),
        );
    }
    validate_command_evidence_shape(&manifest.workspace, "workspace")?;
    let expected_workspace_argv = CANONICAL_WORKSPACE_TEST_ARGV
        .iter()
        .map(|argument| (*argument).to_owned())
        .collect::<Vec<_>>();
    if manifest.workspace.argv != expected_workspace_argv {
        return Err(format!(
            "workspace.argv must exactly equal the canonical command: {expected_workspace_argv:?}"
        ));
    }

    let by_locator = baseline
        .ignored_tests
        .iter()
        .map(|entry| (entry.locator(), entry))
        .collect::<HashMap<_, _>>();
    let guard_entry = by_locator.get(RELEASE_GUARD_LOCATOR).ok_or_else(|| {
        format!("release baseline is missing its live guard entry `{RELEASE_GUARD_LOCATOR}`")
    })?;
    if guard_entry.kind != IgnoreKind::ReleaseGate
        || guard_entry.policy != IgnorePolicy::RunForRelease
    {
        return Err(format!(
            "live release guard `{RELEASE_GUARD_LOCATOR}` must retain release_gate kind and run_for_release policy"
        ));
    }

    let expected_locators = baseline
        .ignored_tests
        .iter()
        .filter(|entry| {
            entry.policy == IgnorePolicy::RunForRelease && entry.locator() != RELEASE_GUARD_LOCATOR
        })
        .map(IgnoredTestBaseline::locator)
        .collect::<HashSet<_>>();
    let mut locators = HashSet::new();
    let mut artifact_paths = HashSet::from([manifest.workspace.artifact_path.clone()]);
    let mut previous_locator: Option<String> = None;
    for receipt in &manifest.run_receipts {
        validate_source_test_identity(&receipt.source_path, &receipt.test_name)
            .map_err(|error| format!("invalid current-run receipt identity: {error}"))?;
        let locator = receipt.locator();
        if previous_locator
            .as_ref()
            .is_some_and(|previous| previous >= &locator)
        {
            return Err(format!(
                "current-run receipts must be strictly sorted and unique: `{locator}` follows `{}`",
                previous_locator.as_deref().unwrap_or_default()
            ));
        }
        previous_locator = Some(locator.clone());
        if locator == RELEASE_GUARD_LOCATOR {
            return Err(
                "the live release guard cannot supply a circular external receipt".to_owned(),
            );
        }
        let entry = by_locator
            .get(&locator)
            .ok_or_else(|| format!("current-run receipt names unknown ignored test `{locator}`"))?;
        if entry.policy != IgnorePolicy::RunForRelease {
            return Err(format!(
                "current-run receipt `{locator}` targets policy `{}` instead of run_for_release",
                entry.policy.as_str()
            ));
        }
        let expected_requirement = blake3::hash(entry.evidence.requirement.as_bytes())
            .to_hex()
            .to_string();
        if receipt.requirement_blake3 != expected_requirement {
            return Err(format!(
                "current-run receipt `{locator}` does not match the current evidence requirement"
            ));
        }
        validate_command_evidence_shape(&receipt.execution, &format!("receipt `{locator}`"))?;
        let expected_argv = expected_current_run_argv(entry)?;
        if receipt.execution.argv != expected_argv {
            return Err(format!(
                "current-run receipt `{locator}` does not use its canonical exact-test command"
            ));
        }
        if !artifact_paths.insert(receipt.execution.artifact_path.clone()) {
            return Err(format!(
                "release evidence artifact paths must be unique: `{}`",
                receipt.execution.artifact_path
            ));
        }
        locators.insert(locator);
    }
    if locators != expected_locators {
        let mut missing = expected_locators
            .difference(&locators)
            .cloned()
            .collect::<Vec<_>>();
        missing.sort();
        return Err(format!(
            "current-run receipts do not cover every non-circular run_for_release entry; missing={missing:?}"
        ));
    }
    Ok(ValidatedCurrentRunReceipts { locators })
}

fn require_tracked_receipt_path(root: &Path, path: &str, description: &str) -> Result<(), String> {
    let status = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(["ls-files", "--error-unmatch", "--", path])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map_err(|error| format!("unable to verify {description} `{path}`: {error}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("{description} must be tracked by Git: `{path}`"))
    }
}

fn require_clean_tracked_path(root: &Path, path: &str, description: &str) -> Result<(), String> {
    require_tracked_receipt_path(root, path, description)?;
    let status = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(["diff", "--quiet", "HEAD", "--", path])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map_err(|error| format!("unable to verify {description} is clean: {error}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!(
            "{description} must match the current commit exactly: `{path}`"
        ))
    }
}

fn require_regular_non_symlink_path(
    root: &Path,
    path: &str,
    description: &str,
) -> Result<(), String> {
    let metadata = fs::symlink_metadata(root.join(path))
        .map_err(|error| format!("unable to inspect {description} `{path}`: {error}"))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(format!(
            "{description} must be a regular non-symlink file: `{path}`"
        ));
    }
    Ok(())
}

fn read_regular_evidence_file(
    root: &Path,
    path: &str,
    expected_digest: &str,
    description: &str,
) -> Result<Vec<u8>, String> {
    require_clean_tracked_path(root, path, description)?;
    require_regular_non_symlink_path(root, path, description)?;
    let absolute_path = root.join(path);
    let bytes = fs::read(&absolute_path)
        .map_err(|error| format!("unable to read {description} `{path}`: {error}"))?;
    let actual_digest = blake3::hash(&bytes).to_hex().to_string();
    if actual_digest != expected_digest {
        return Err(format!("{description} content hash mismatch: `{path}`"));
    }
    Ok(bytes)
}

fn release_status_record_is_allowed(record: &[u8]) -> bool {
    record.starts_with(b"!! target/")
}

fn require_pristine_release_checkout(root: &Path) -> Result<(), String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(root)
        .args([
            "status",
            "--porcelain=v1",
            "-z",
            "--untracked-files=all",
            "--ignored=matching",
        ])
        .output()
        .map_err(|error| format!("unable to inspect release worktree state: {error}"))?;
    if !output.status.success() {
        return Err("unable to inspect release worktree state".to_owned());
    }
    if output
        .stdout
        .split(|byte| *byte == 0)
        .filter(|record| !record.is_empty())
        .all(release_status_record_is_allowed)
    {
        Ok(())
    } else {
        Err(
            "release evidence requires a pristine checkout; only ignored target/ build output is allowed"
                .to_owned(),
        )
    }
}

fn changed_paths_between(root: &Path, base: &str, head: &str) -> Result<HashSet<String>, String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(root)
        .args([
            "diff",
            "--no-renames",
            "--name-only",
            "-z",
            base,
            head,
            "--",
        ])
        .output()
        .map_err(|error| format!("unable to inspect release evidence commit delta: {error}"))?;
    if !output.status.success() {
        return Err("unable to inspect release evidence commit delta".to_owned());
    }
    let mut paths = HashSet::new();
    for raw_path in output.stdout.split(|byte| *byte == 0) {
        if raw_path.is_empty() {
            continue;
        }
        let raw_path = std::str::from_utf8(raw_path)
            .map_err(|error| format!("release evidence delta path is not UTF-8: {error}"))?;
        let normalized = normalize_source_path(Path::new(raw_path))?;
        if normalized != raw_path || !paths.insert(normalized) {
            return Err(format!(
                "release evidence delta contains a noncanonical or duplicate path: `{raw_path}`"
            ));
        }
    }
    Ok(paths)
}

fn verify_tested_commit(root: &Path, tested_commit: &str, head: &str) -> Result<(), String> {
    if tested_commit == head {
        return Err(
            "release evidence must be committed in an evidence-only descendant of tested_commit"
                .to_owned(),
        );
    }
    let commit_object = format!("{tested_commit}^{{commit}}");
    let exists = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(["cat-file", "-e", &commit_object])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map_err(|error| format!("unable to verify tested_commit: {error}"))?;
    if !exists.success() {
        return Err(format!(
            "release evidence tested_commit does not resolve to a commit: `{tested_commit}`"
        ));
    }
    let ancestor = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(["merge-base", "--is-ancestor", tested_commit, head])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map_err(|error| format!("unable to compare tested_commit with current commit: {error}"))?;
    if ancestor.success() {
        Ok(())
    } else {
        Err(format!(
            "release evidence tested_commit `{tested_commit}` is not an ancestor of `{head}`"
        ))
    }
}

fn validate_single_test_transcript(
    transcript: &str,
    entry: &IgnoredTestBaseline,
) -> Result<(), String> {
    let test_name = &entry.test_name;
    let counts = parse_workspace_test_counts(transcript)?;
    if counts
        != (RegressionCounts {
            total_tests: 1,
            passed: 1,
            failed: 0,
            ignored: 0,
        })
    {
        return Err(format!(
            "exact-test receipt for `{test_name}` must report exactly one selected passing test"
        ));
    }
    let expected_header_prefix = match expected_current_run_target(entry)? {
        CargoTestTarget::Library => "     Running unittests src/lib.rs (".to_owned(),
        CargoTestTarget::Integration(target) => {
            format!("     Running tests/{target}.rs (")
        }
    };
    let target_headers = transcript
        .lines()
        .filter(|line| cargo_target_section(line).is_some())
        .collect::<Vec<_>>();
    if !matches!(
        target_headers.as_slice(),
        [header] if header.starts_with(&expected_header_prefix)
    ) {
        return Err(format!(
            "exact-test receipt for `{test_name}` does not identify its canonical Cargo target"
        ));
    }
    let expected_result = format!("test {test_name} ... ok");
    if transcript
        .lines()
        .filter(|line| line.trim() == expected_result)
        .count()
        != 1
    {
        return Err(format!(
            "exact-test receipt does not contain one matching success line for `{test_name}`"
        ));
    }
    Ok(())
}

fn resolve_current_head(root: &Path) -> Result<String, String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(["rev-parse", "--verify", "HEAD"])
        .output()
        .map_err(|error| format!("unable to resolve current release commit: {error}"))?;
    if !output.status.success() {
        return Err("unable to resolve current release commit".to_owned());
    }
    let head = std::str::from_utf8(&output.stdout)
        .map_err(|error| format!("current release commit is not valid UTF-8: {error}"))?
        .trim_end_matches(['\r', '\n']);
    if head.len() != 40 || !is_lowercase_hex(head) {
        return Err(format!(
            "current release commit is not a canonical full Git object name: `{head}`"
        ));
    }
    Ok(head.to_owned())
}

fn load_release_evidence_manifest(
    root: &Path,
    head: &str,
    baseline: &RegressionBaseline,
) -> Result<ValidatedReleaseEvidence, String> {
    let manifest_path = std::env::var(EVIDENCE_MANIFEST_ENV)
        .map_err(|error| format!("missing {EVIDENCE_MANIFEST_ENV}: {error}"))?;
    let expected_manifest_digest = parse_required_lowercase_hex(EVIDENCE_MANIFEST_BLAKE3_ENV, 64)?;
    load_release_evidence_manifest_from_path(
        root,
        head,
        baseline,
        &manifest_path,
        &expected_manifest_digest,
    )
}

fn load_release_evidence_manifest_from_path(
    root: &Path,
    head: &str,
    baseline: &RegressionBaseline,
    manifest_path: &str,
    expected_manifest_digest: &str,
) -> Result<ValidatedReleaseEvidence, String> {
    canonical_evidence_path(manifest_path, "release evidence manifest path")?;
    let manifest_bytes = read_regular_evidence_file(
        root,
        manifest_path,
        expected_manifest_digest,
        "release evidence manifest",
    )?;
    let manifest = serde_json::from_slice::<ReleaseEvidenceManifest>(&manifest_bytes)
        .map_err(|error| format!("unable to parse release evidence manifest: {error}"))?;
    let current_run_receipts = validate_release_evidence_manifest(&manifest, baseline)?;
    for entry in baseline.ignored_tests.iter().filter(|entry| {
        entry.policy == IgnorePolicy::RunForRelease && entry.locator() != RELEASE_GUARD_LOCATOR
    }) {
        validate_cargo_target_mapping(root, entry)?;
    }
    verify_tested_commit(root, &manifest.tested_commit, head)?;

    let baseline_commit_object = format!("{}^{{commit}}", baseline.baseline_commit);
    let baseline_is_ancestor = Command::new("git")
        .arg("-C")
        .arg(root)
        .args([
            "merge-base",
            "--is-ancestor",
            &baseline_commit_object,
            &manifest.tested_commit,
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map_err(|error| format!("unable to compare baseline and tested commits: {error}"))?;
    if !baseline_is_ancestor.success() {
        return Err(format!(
            "baseline_commit `{}` is not an ancestor of tested_commit `{}`",
            baseline.baseline_commit, manifest.tested_commit
        ));
    }

    let mut evidence_paths = HashSet::from([
        manifest_path.to_owned(),
        manifest.workspace.artifact_path.clone(),
    ]);
    evidence_paths.extend(
        manifest
            .run_receipts
            .iter()
            .map(|receipt| receipt.execution.artifact_path.clone()),
    );
    let changed_paths = changed_paths_between(root, &manifest.tested_commit, head)?;
    if changed_paths != evidence_paths {
        let mut unexpected = changed_paths
            .difference(&evidence_paths)
            .cloned()
            .collect::<Vec<_>>();
        unexpected.sort();
        let mut stale = evidence_paths
            .difference(&changed_paths)
            .cloned()
            .collect::<Vec<_>>();
        stale.sort();
        return Err(format!(
            "tested_commit..HEAD must be an exact evidence-only delta; unexpected={unexpected:?} unchanged_evidence={stale:?}"
        ));
    }
    require_pristine_release_checkout(root)?;

    let workspace_bytes = read_regular_evidence_file(
        root,
        &manifest.workspace.artifact_path,
        &manifest.workspace.artifact_blake3,
        "workspace transcript",
    )?;
    let workspace_transcript = String::from_utf8(workspace_bytes)
        .map_err(|error| format!("workspace transcript is not UTF-8: {error}"))?;
    let workspace_counts = parse_workspace_test_counts(&workspace_transcript)?;

    for receipt in &manifest.run_receipts {
        let artifact = read_regular_evidence_file(
            root,
            &receipt.execution.artifact_path,
            &receipt.execution.artifact_blake3,
            &format!("current-run receipt `{}`", receipt.locator()),
        )?;
        let transcript = String::from_utf8(artifact).map_err(|error| {
            format!(
                "current-run receipt `{}` is not UTF-8: {error}",
                receipt.locator()
            )
        })?;
        let entry = baseline
            .ignored_tests
            .iter()
            .find(|entry| entry.locator() == receipt.locator())
            .ok_or_else(|| {
                format!(
                    "validated current-run receipt disappeared from baseline: `{}`",
                    receipt.locator()
                )
            })?;
        validate_single_test_transcript(&transcript, entry)?;
    }

    Ok(ValidatedReleaseEvidence {
        tested_commit: manifest.tested_commit,
        workspace_transcript,
        workspace_counts,
        current_run_receipts,
    })
}

fn parse_regression_baseline(path: &Path) -> Result<RegressionBaseline, String> {
    let bytes = fs::read(path)
        .map_err(|error| format!("unable to read baseline at {}: {error}", path.display()))?;
    let baseline = serde_json::from_slice::<RegressionBaseline>(&bytes).map_err(|error| {
        format!(
            "unable to parse baseline JSON at {}: {error}",
            path.display()
        )
    })?;
    baseline
        .validate()
        .map_err(|error| format!("invalid regression baseline at {}: {error}", path.display()))?;
    Ok(baseline)
}

fn load_regression_baseline(
    path: &Path,
    root: &Path,
    head: &str,
) -> Result<RegressionBaseline, String> {
    let relative_path = path.strip_prefix(root).map_err(|error| {
        format!(
            "regression baseline path is outside the repository root: {}: {error}",
            path.display()
        )
    })?;
    let relative_path = normalize_source_path(relative_path)?;
    if relative_path != REGRESSION_BASELINE_PATH {
        return Err(format!(
            "release regression baseline must use `{REGRESSION_BASELINE_PATH}`, found `{relative_path}`"
        ));
    }
    require_clean_tracked_path(root, &relative_path, "release regression baseline")?;
    require_regular_non_symlink_path(root, &relative_path, "release regression baseline")?;
    let baseline = parse_regression_baseline(path)?;
    let commit_object = format!("{}^{{commit}}", baseline.baseline_commit);
    let status = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(["cat-file", "-e", &commit_object])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map_err(|error| format!("unable to verify baseline_commit: {error}"))?;
    if !status.success() {
        return Err(format!(
            "baseline_commit `{}` does not resolve to a Git commit in {}",
            baseline.baseline_commit,
            root.display()
        ));
    }
    let baseline_ancestor_status = Command::new("git")
        .arg("-C")
        .arg(root)
        .args([
            "merge-base",
            "--is-ancestor",
            &baseline.baseline_commit,
            head,
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map_err(|error| {
            format!("unable to compare baseline_commit with current commit: {error}")
        })?;
    if !baseline_ancestor_status.success() {
        return Err(format!(
            "baseline_commit `{}` is not an ancestor of current commit `{head}`",
            baseline.baseline_commit
        ));
    }

    for entry in &baseline.ignored_tests {
        let Some(receipt) = &entry.evidence.receipt else {
            continue;
        };
        let receipt_commit = format!("{}^{{commit}}", receipt.source_commit);
        let commit_status = Command::new("git")
            .arg("-C")
            .arg(root)
            .args(["cat-file", "-e", &receipt_commit])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map_err(|error| format!("unable to verify receipt source commit: {error}"))?;
        if !commit_status.success() {
            return Err(format!(
                "ignored-test receipt for `{}` names a missing source commit {}",
                entry.locator(),
                receipt.source_commit
            ));
        }
        let ancestor_status = Command::new("git")
            .arg("-C")
            .arg(root)
            .args(["merge-base", "--is-ancestor", &receipt.source_commit, head])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map_err(|error| format!("unable to compare receipt source commit: {error}"))?;
        if !ancestor_status.success() {
            return Err(format!(
                "ignored-test receipt source commit {} is not an ancestor of current commit {head}",
                receipt.source_commit
            ));
        }
        require_tracked_receipt_path(root, &entry.source_path, "receipt-covered source")?;
        require_tracked_receipt_path(root, &receipt.artifact_path, "receipt artifact")?;
        if let Some(parent_source_path) = &receipt.parent_source_path {
            require_tracked_receipt_path(root, parent_source_path, "receipt parent source")?;
        }
        let mut diff_command = Command::new("git");
        diff_command
            .arg("-C")
            .arg(root)
            .args(["diff", "--quiet", &receipt.source_commit, head, "--"])
            .arg(&entry.source_path);
        if let Some(parent_source_path) = &receipt.parent_source_path {
            diff_command.arg(parent_source_path);
        }
        let diff_status = diff_command
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map_err(|error| format!("unable to compare receipt-covered sources: {error}"))?;
        match diff_status.code() {
            Some(0) => {}
            Some(1) => {
                return Err(format!(
                    "ignored-test receipt for `{}` is stale because its covered source changed",
                    entry.locator()
                ));
            }
            _ => return Err("unable to compare receipt-covered sources".to_owned()),
        }
        let mut worktree_command = Command::new("git");
        worktree_command
            .arg("-C")
            .arg(root)
            .args(["diff", "--quiet", "HEAD", "--"])
            .arg(&entry.source_path)
            .arg(&receipt.artifact_path);
        if let Some(parent_source_path) = &receipt.parent_source_path {
            worktree_command.arg(parent_source_path);
        }
        let worktree_status = worktree_command
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map_err(|error| format!("unable to verify receipt inputs are clean: {error}"))?;
        if !worktree_status.success() {
            return Err(format!(
                "ignored-test receipt for `{}` has uncommitted source or artifact changes",
                entry.locator()
            ));
        }
        let artifact_path = root.join(&receipt.artifact_path);
        let metadata = fs::symlink_metadata(&artifact_path).map_err(|error| {
            format!(
                "unable to inspect ignored-test receipt artifact `{}`: {error}",
                receipt.artifact_path
            )
        })?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(format!(
                "ignored-test receipt artifact must be a regular non-symlink file: `{}`",
                receipt.artifact_path
            ));
        }
        let artifact = fs::read(&artifact_path).map_err(|error| {
            format!(
                "unable to read ignored-test receipt artifact `{}`: {error}",
                receipt.artifact_path
            )
        })?;
        let actual_digest = blake3::hash(&artifact).to_hex().to_string();
        if actual_digest != receipt.artifact_blake3 {
            return Err(format!(
                "ignored-test receipt artifact digest mismatch for `{}`",
                receipt.artifact_path
            ));
        }
    }
    Ok(baseline)
}

fn parse_count_segment(segment: &str, label: &str) -> Option<u64> {
    let suffix = format!(" {label}");
    let value_prefix = segment.trim().strip_suffix(&suffix)?;
    let count_text = value_prefix.split_whitespace().last()?;
    count_text.parse::<u64>().ok()
}

fn parse_summary_line(line: &str) -> Option<RegressionCounts> {
    let result = line.strip_prefix("test result: ")?;
    let outcome = result.split_whitespace().next()?;
    if !matches!(outcome, "ok." | "FAILED.") {
        return None;
    }

    let mut passed = None;
    let mut failed = None;
    let mut ignored = None;

    for segment in line.split(';') {
        if passed.is_none() {
            passed = parse_count_segment(segment, "passed");
        }
        if failed.is_none() {
            failed = parse_count_segment(segment, "failed");
        }
        if ignored.is_none() {
            ignored = parse_count_segment(segment, "ignored");
        }
    }

    let passed = passed?;
    let failed = failed?;
    let ignored = ignored?;
    if (outcome == "ok." && failed != 0) || (outcome == "FAILED." && failed == 0) {
        return None;
    }
    let total_tests = passed.checked_add(failed)?.checked_add(ignored)?;

    Some(RegressionCounts {
        total_tests,
        passed,
        failed,
        ignored,
    })
}

fn cargo_target_section(line: &str) -> Option<&str> {
    if let Some(section) = line.strip_prefix("     Running ") {
        if !section.is_empty() && section.contains(" (") && section.ends_with(')') {
            return Some(section);
        }
    }

    let section = line.strip_prefix("   Doc-tests ")?;
    (!section.trim().is_empty() && section == section.trim_end()).then_some(section)
}

fn parse_workspace_test_counts(output: &str) -> Result<RegressionCounts, String> {
    if output.contains('\u{1b}') {
        return Err("workspace transcript contains ANSI escape sequences".to_owned());
    }

    let mut totals = RegressionCounts::zero();
    let mut active_section: Option<String> = None;
    let mut active_summary: Option<RegressionCounts> = None;

    for line in output.lines() {
        if let Some(section_header) = cargo_target_section(line) {
            if let Some(section) = active_section.take() {
                let summary = active_summary.take().ok_or_else(|| {
                    format!("cargo target section `{section}` had no test-result summary")
                })?;
                totals.checked_add(summary)?;
            }
            active_section = Some(section_header.to_owned());
            continue;
        }

        if line.starts_with("test result: ") {
            let parsed = parse_summary_line(line)
                .ok_or_else(|| format!("malformed cargo test summary line: {line}"))?;
            if active_section.is_none() {
                return Err(format!(
                    "cargo test summary appeared outside a target section: {line}"
                ));
            }
            // Subprocess helpers can emit their own summaries into a parent
            // target's captured output. The outer harness summary is last and
            // is therefore the only authoritative count for this section.
            active_summary = Some(parsed);
        }
    }

    let section = active_section
        .ok_or_else(|| "no cargo test target sections were found in output".to_owned())?;
    let summary = active_summary
        .ok_or_else(|| format!("cargo target section `{section}` had no test-result summary"))?;
    totals.checked_add(summary)?;

    Ok(totals)
}

fn parse_required_lowercase_hex(name: &str, expected_len: usize) -> Result<String, String> {
    let value = std::env::var(name).map_err(|error| format!("missing {name}: {error}"))?;
    if value.len() != expected_len || !is_lowercase_hex(&value) {
        return Err(format!(
            "{name} must be exactly {expected_len} lowercase hexadecimal digits"
        ));
    }
    Ok(value)
}

fn validate_live_release_guard_invocation(arguments: &[String]) -> Result<(), String> {
    let test_name = RELEASE_GUARD_LOCATOR
        .rsplit_once("::")
        .map(|(_, test_name)| test_name)
        .expect("release guard locator contains a test name");
    if !arguments.iter().any(|argument| argument == test_name)
        || !arguments.iter().any(|argument| argument == "--exact")
        || !arguments.iter().any(|argument| argument == "--ignored")
        || arguments.iter().any(|argument| argument == "--skip")
    {
        return Err(
            "live release guard must be selected by its exact name with --exact and --ignored"
                .to_owned(),
        );
    }
    Ok(())
}

fn as_i64(value: i128) -> i64 {
    match i64::try_from(value) {
        Ok(v) => v,
        Err(_) => {
            if value.is_negative() {
                i64::MIN
            } else {
                i64::MAX
            }
        }
    }
}

fn compare_against_baseline(
    baseline: &RegressionBaseline,
    actual: &RegressionCounts,
) -> RegressionReport {
    let delta_total = i128::from(actual.total_tests) - i128::from(baseline.total_tests);
    let delta_passed = i128::from(actual.passed) - i128::from(baseline.passed);
    let delta_failed = i128::from(actual.failed) - i128::from(baseline.failed);
    let delta_ignored = i128::from(actual.ignored) - i128::from(baseline.ignored);

    let delta = RegressionDelta {
        delta_total: as_i64(delta_total),
        delta_passed: as_i64(delta_passed),
        delta_failed: as_i64(delta_failed),
        delta_ignored: as_i64(delta_ignored),
        new_tests: as_i64(delta_total),
    };

    let mut reasons = Vec::new();
    if actual.failed > baseline.failed {
        reasons.push(format!(
            "failed increased from {} to {}",
            baseline.failed, actual.failed
        ));
    }
    if actual.passed < baseline.passed {
        reasons.push(format!(
            "passed decreased from {} to {}",
            baseline.passed, actual.passed
        ));
    }
    if actual.total_tests < baseline.total_tests {
        reasons.push(format!(
            "total tests decreased from {} to {}",
            baseline.total_tests, actual.total_tests
        ));
    }

    let pass = reasons.is_empty();
    let reason = if pass { None } else { Some(reasons.join("; ")) };

    RegressionReport {
        pass,
        delta,
        reason,
    }
}

fn extract_failed_tests(output: &str) -> Vec<String> {
    let mut failed = Vec::new();
    for line in output.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("test ") && trimmed.ends_with(" ... FAILED") {
            failed.push(trimmed.to_owned());
        }
    }
    failed
}

fn compare_ignored_test_taxonomy(
    expected: &[IgnoredTestBaseline],
    actual: &[IgnoredTestSource],
) -> Vec<String> {
    let mut expected = expected.iter().collect::<Vec<_>>();
    expected.sort_by_key(|entry| entry.locator());
    let mut actual = actual.iter().collect::<Vec<_>>();
    actual.sort_by_key(|entry| entry.locator());

    let mut mismatches = Vec::new();
    let mut expected_index = 0;
    let mut actual_index = 0;
    while expected_index < expected.len() || actual_index < actual.len() {
        match (expected.get(expected_index), actual.get(actual_index)) {
            (Some(expected_entry), Some(actual_entry)) => {
                let expected_locator = expected_entry.locator();
                let actual_locator = actual_entry.locator();
                match expected_locator.cmp(&actual_locator) {
                    std::cmp::Ordering::Less => {
                        mismatches.push(format!(
                            "missing {expected_locator}: baseline entry has no matching source ignore"
                        ));
                        expected_index += 1;
                    }
                    std::cmp::Ordering::Greater => {
                        mismatches.push(format!(
                            "unclassified {actual_locator}: reason={:?} cfg_condition={:?}",
                            actual_entry.reason, actual_entry.cfg_condition
                        ));
                        actual_index += 1;
                    }
                    std::cmp::Ordering::Equal => {
                        let expected_source = expected_entry.source_identity();
                        let mut changed_fields = Vec::new();
                        if expected_source.reason != actual_entry.reason {
                            changed_fields.push(format!(
                                "reason expected={:?} actual={:?}",
                                expected_source.reason, actual_entry.reason
                            ));
                        }
                        if expected_source.cfg_condition != actual_entry.cfg_condition {
                            changed_fields.push(format!(
                                "cfg_condition expected={:?} actual={:?}",
                                expected_source.cfg_condition, actual_entry.cfg_condition
                            ));
                        }
                        if !changed_fields.is_empty() {
                            mismatches.push(format!(
                                "changed {expected_locator}: {}",
                                changed_fields.join("; ")
                            ));
                        }
                        expected_index += 1;
                        actual_index += 1;
                    }
                }
            }
            (Some(expected_entry), None) => {
                let locator = expected_entry.locator();
                mismatches.push(format!(
                    "missing {locator}: baseline entry has no matching source ignore"
                ));
                expected_index += 1;
            }
            (None, Some(actual_entry)) => {
                mismatches.push(format!(
                    "unclassified {}: reason={:?} cfg_condition={:?}",
                    actual_entry.locator(),
                    actual_entry.reason,
                    actual_entry.cfg_condition
                ));
                actual_index += 1;
            }
            (None, None) => break,
        }
    }
    mismatches
}

fn ignored_test_release_blockers(
    entries: &[IgnoredTestBaseline],
    current_run_receipts: &ValidatedCurrentRunReceipts,
    live_release_guard: bool,
) -> Vec<String> {
    entries
        .iter()
        .filter_map(|entry| {
            let locator = entry.locator();
            match entry.policy {
                IgnorePolicy::BlockRelease => Some(format!(
                    "{locator}: block_release policy remains unresolved"
                )),
                IgnorePolicy::RunForRelease
                    if !(current_run_receipts.contains(&locator)
                        || (live_release_guard && locator == RELEASE_GUARD_LOCATOR)) =>
                {
                    Some(format!(
                        "{locator}: run_for_release lacks validated current-run evidence"
                    ))
                }
                IgnorePolicy::CoveredByParent => Some(format!(
                    "{locator}: covered_by_parent evidence remains non-authoritative until parent execution and acyclicity are machine-validated"
                )),
                IgnorePolicy::RunForRelease | IgnorePolicy::Exempt => None,
            }
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReleaseGateEvaluation {
    aggregate: RegressionReport,
    uninspected_sources: Vec<String>,
    inventory_soundness_limitations: Vec<String>,
    taxonomy_mismatches: Vec<String>,
    policy_blockers: Vec<String>,
}

impl ReleaseGateEvaluation {
    fn passes(&self) -> bool {
        self.aggregate.pass
            && self.uninspected_sources.is_empty()
            && self.inventory_soundness_limitations.is_empty()
            && self.taxonomy_mismatches.is_empty()
            && self.policy_blockers.is_empty()
    }

    fn failure_summary(&self) -> String {
        let mut reasons = Vec::new();
        if let Some(reason) = &self.aggregate.reason {
            reasons.push(reason.clone());
        }
        if !self.uninspected_sources.is_empty() {
            reasons.push(format!(
                "{} tracked Rust source(s) remain explicitly uninspected",
                self.uninspected_sources.len()
            ));
        }
        if !self.inventory_soundness_limitations.is_empty() {
            reasons.push(format!(
                "ignored-test inventory has {} unresolved soundness limitation(s)",
                self.inventory_soundness_limitations.len()
            ));
        }
        if !self.taxonomy_mismatches.is_empty() {
            reasons.push(format!(
                "ignored-test taxonomy has {} mismatch(es)",
                self.taxonomy_mismatches.len()
            ));
        }
        if !self.policy_blockers.is_empty() {
            reasons.push(format!(
                "ignored-test policy has {} unresolved release blocker(s)",
                self.policy_blockers.len()
            ));
        }
        if reasons.is_empty() {
            "release gate failed without a classified reason".to_owned()
        } else {
            reasons.join("; ")
        }
    }
}

fn evaluate_release_gate(
    baseline: &RegressionBaseline,
    actual_counts: &RegressionCounts,
    inventory: &RepositoryIgnoreInventory,
    current_run_receipts: &ValidatedCurrentRunReceipts,
    live_release_guard: bool,
) -> ReleaseGateEvaluation {
    ReleaseGateEvaluation {
        aggregate: compare_against_baseline(baseline, actual_counts),
        uninspected_sources: inventory.uninspected_sources.clone(),
        inventory_soundness_limitations: inventory.soundness_limitations.clone(),
        taxonomy_mismatches: compare_ignored_test_taxonomy(
            &baseline.ignored_tests,
            &inventory.records,
        ),
        policy_blockers: ignored_test_release_blockers(
            &baseline.ignored_tests,
            current_run_receipts,
            live_release_guard,
        ),
    }
}

fn sample_ignored_baseline(source_path: &str, test_name: &str) -> IgnoredTestBaseline {
    IgnoredTestBaseline {
        source_path: source_path.to_owned(),
        test_name: test_name.to_owned(),
        reason: "tracked gap".to_owned(),
        cfg_condition: None,
        kind: IgnoreKind::KnownBug,
        policy: IgnorePolicy::BlockRelease,
        evidence: IgnoreEvidence {
            requirement: "close the tracked correctness gap with an exact keeper".to_owned(),
            receipt: None,
        },
    }
}

fn sample_ignore_receipt(parent: Option<(&str, &str)>) -> IgnoreEvidenceReceipt {
    let (parent_source_path, parent_test_name) = parent.map_or((None, None), |(path, test)| {
        (Some(path.to_owned()), Some(test.to_owned()))
    });
    IgnoreEvidenceReceipt {
        source_commit: "a".repeat(40),
        artifact_path: "tests/artifacts/receipt.json".to_owned(),
        artifact_blake3: "b".repeat(64),
        parent_source_path,
        parent_test_name,
    }
}

fn sample_command_evidence(argv: Vec<String>, artifact_path: &str) -> CommandEvidence {
    CommandEvidence {
        argv,
        exit_status: 0,
        capture_status: 0,
        artifact_path: artifact_path.to_owned(),
        artifact_blake3: "b".repeat(64),
    }
}

fn sample_release_evidence() -> (RegressionBaseline, ReleaseEvidenceManifest) {
    let mut release_run = sample_ignored_baseline(
        "crates/fsqlite-e2e/tests/manual_release.rs",
        "manual_release_case",
    );
    release_run.kind = IgnoreKind::Stress;
    release_run.policy = IgnorePolicy::RunForRelease;
    let mut live_guard = sample_ignored_baseline(
        "crates/fsqlite-harness/tests/phase5_regression_guard.rs",
        "phase5_regression_guard_full_workspace_against_baseline",
    );
    live_guard.kind = IgnoreKind::ReleaseGate;
    live_guard.policy = IgnorePolicy::RunForRelease;
    let requirement_blake3 = blake3::hash(release_run.evidence.requirement.as_bytes())
        .to_hex()
        .to_string();
    let execution = sample_command_evidence(
        expected_current_run_argv(&release_run).expect("sample command must be supported"),
        "tests/artifacts/release-evidence/manual-release.txt",
    );
    let manifest = ReleaseEvidenceManifest {
        schema_version: EVIDENCE_SCHEMA_VERSION,
        tested_commit: "a".repeat(40),
        workspace: sample_command_evidence(
            CANONICAL_WORKSPACE_TEST_ARGV
                .iter()
                .map(|argument| (*argument).to_owned())
                .collect(),
            "tests/artifacts/release-evidence/workspace.txt",
        ),
        run_receipts: vec![CurrentRunReceipt {
            source_path: release_run.source_path.clone(),
            test_name: release_run.test_name.clone(),
            requirement_blake3,
            execution,
        }],
    };
    let baseline = RegressionBaseline {
        as_of_phase: "checkpoint_1".to_owned(),
        total_tests: 1,
        passed: 1,
        failed: 0,
        ignored: 0,
        baseline_commit: "deadbeef".to_owned(),
        ignored_tests: vec![release_run, live_guard],
    };
    (baseline, manifest)
}

#[test]
fn test_ignore_source_collector_records_direct_reason_and_normalized_identity() {
    let source = r#"
#[test]
#[ignore = "tracked correctness gap"]
fn direct_case() {}
"#;
    let records =
        collect_ignored_tests(Path::new("./crates/example/../example/src\\lib.rs"), source)
            .expect("direct reasoned ignore should be collected");
    assert_eq!(
        records,
        vec![IgnoredTestSource {
            source_path: "crates/example/src/lib.rs".to_owned(),
            test_name: "direct_case".to_owned(),
            reason: "tracked correctness gap".to_owned(),
            cfg_condition: None,
        }]
    );
    assert_eq!(
        records[0].locator(),
        "crates/example/src/lib.rs::direct_case"
    );
}

#[test]
fn test_ignore_source_collector_parses_multiline_cfg_attr() {
    let source = r#"
#[test]
#[cfg_attr(
    all(target_os = "linux", feature = "slow-tests"),
    ignore = "requires the slow-test fixture",
)]
fn conditional_case() {}
"#;
    let records = collect_ignored_tests(Path::new("tests/conditional.rs"), source)
        .expect("multiline cfg_attr ignore should be collected");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].test_name, "conditional_case");
    assert_eq!(
        records[0].cfg_condition.as_deref(),
        Some("all(feature=\"slow-tests\",target_os=\"linux\")")
    );
}

#[test]
fn test_ignore_source_collector_canonicalizes_nested_all_any_not_conditions() {
    let source = r#"
#[test]
#[cfg_attr(
    all(unix, any(feature = "b", not(miri), feature = "a")),
    cfg_attr(
        not(any(target_os = "windows", target_os = "macos")),
        ignore = "nested conditional gap",
    ),
)]
fn nested_case() {}
"#;
    let records = collect_ignored_tests(Path::new("tests/nested.rs"), source)
        .expect("nested cfg_attr ignore should be collected");
    assert_eq!(records.len(), 1);
    assert_eq!(
        records[0].cfg_condition.as_deref(),
        Some(
            "all(any(feature=\"a\",feature=\"b\",not(miri)),not(any(target_os=\"macos\",target_os=\"windows\")),unix)"
        )
    );
}

#[test]
fn test_ignore_source_collector_combines_file_module_function_and_ignore_cfg() {
    let source = r#"
#![cfg(feature = "file")]

#[cfg(unix)]
mod suite {
    #[cfg(feature = "module")]
    mod nested {
        #[test]
        #[cfg(feature = "function-b")]
        #[cfg(feature = "function-a")]
        #[cfg_attr(feature = "slow", ignore = "conditional stress case")]
        fn conditional_case() {}
    }
}
"#;
    let records = collect_ignored_tests(Path::new("tests/cfg_context.rs"), source)
        .expect("file, module, function, and ignore cfgs should combine");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].test_name, "suite::nested::conditional_case");
    assert_eq!(
        records[0].cfg_condition.as_deref(),
        Some(
            "all(feature=\"file\",feature=\"function-a\",feature=\"function-b\",feature=\"module\",feature=\"slow\",unix)"
        )
    );
}

#[test]
fn test_ignore_source_collector_models_cfg_attr_availability_and_restores_siblings() {
    let source = r#"
#[cfg_attr(feature = "portable", cfg(unix))]
mod gated {
    #[test]
    #[cfg_attr(feature = "backend", cfg(target_os = "linux"))]
    #[cfg_attr(feature = "slow", ignore = "conditional case")]
    fn conditional_case() {}
}

#[test]
#[ignore = "ungated sibling"]
fn sibling_case() {}
"#;
    let records = collect_ignored_tests(Path::new("tests/cfg_attr_context.rs"), source)
        .expect("cfg_attr availability should be symbolic and sibling-local");
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].test_name, "gated::conditional_case");
    assert_eq!(
        records[0].cfg_condition.as_deref(),
        Some(
            "all(any(not(feature=\"backend\"),target_os=\"linux\"),any(not(feature=\"portable\"),unix),feature=\"slow\")"
        )
    );
    assert_eq!(records[1].test_name, "sibling_case");
    assert_eq!(records[1].cfg_condition, None);
}

#[test]
fn test_ignore_source_collector_models_nested_and_multi_gate_cfg_attrs() {
    let source = r#"
#[cfg_attr(
    feature = "outer",
    cfg_attr(unix, cfg(target_os = "linux")),
)]
mod nested {
    #[test]
    #[ignore = "nested availability"]
    fn nested_case() {}
}

#[test]
#[cfg_attr(
    feature = "x",
    cfg(unix),
    cfg(target_pointer_width = "64"),
)]
#[ignore = "multiple availability gates"]
fn multiple_gate_case() {}
"#;
    let records = collect_ignored_tests(Path::new("tests/nested_availability.rs"), source)
        .expect("nested and multi-gate cfg_attr should be modeled");
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].test_name, "multiple_gate_case");
    assert_eq!(
        records[0].cfg_condition.as_deref(),
        Some("any(all(target_pointer_width=\"64\",unix),not(feature=\"x\"))")
    );
    assert_eq!(records[1].test_name, "nested::nested_case");
    assert_eq!(
        records[1].cfg_condition.as_deref(),
        Some("any(not(feature=\"outer\"),not(unix),target_os=\"linux\")")
    );
}

#[test]
fn test_ignore_source_collector_rejects_malformed_cfg_gate() {
    let source = r#"
#[test]
#[cfg(unix, windows)]
#[ignore = "malformed availability"]
fn malformed_case() {}
"#;
    let error = collect_ignored_tests(Path::new("tests/malformed_cfg.rs"), source)
        .expect_err("cfg with multiple predicates must fail closed");
    assert!(error.contains("cfg requires exactly one predicate"));
}

#[test]
fn test_ignore_source_collector_ignores_comments_docs_strings_and_literal_only_macros() {
    let source = r##"
// #[ignore = "comment text is not an attribute"]
/// #[ignore = "doc text is not an attribute"]
#[doc = "#[ignore = \"doc literal is not an attribute\"]"]
const DOCUMENTED: &str = "#[ignore = \"ordinary string\"]";

macro_rules! literal_only {
    () => { r#"#[cfg_attr(unix, ignore = \"macro string\")]"# };
}

#[test]
fn active_case() {
    let _ = DOCUMENTED;
}
"##;
    let records = collect_ignored_tests(Path::new("tests/noise.rs"), source)
        .expect("ignore-like text in literals and documentation must not fail the audit");
    assert!(records.is_empty());
}

#[test]
fn test_ignore_source_collector_distinguishes_same_name_in_inline_modules() {
    let source = r#"
mod beta {
    mod nested {
        #[test]
        #[ignore = "beta gap"]
        fn same_name() {}
    }
}

mod alpha {
    #[test]
    #[ignore = "alpha gap"]
    fn same_name() {}
}
"#;
    let records = collect_ignored_tests(Path::new("tests/modules.rs"), source)
        .expect("inline module paths should disambiguate test names");
    let identities = records
        .iter()
        .map(IgnoredTestSource::locator)
        .collect::<Vec<_>>();
    assert_eq!(
        identities,
        vec![
            "tests/modules.rs::alpha::same_name",
            "tests/modules.rs::beta::nested::same_name",
        ]
    );
}

#[test]
fn test_ignore_source_collector_rejects_duplicate_and_overlapping_ignores() {
    let duplicate = r#"
#[test]
#[ignore = "same gap"]
#[ignore = "same gap"]
fn duplicate_case() {}
"#;
    let error = collect_ignored_tests(Path::new("tests/duplicate.rs"), duplicate)
        .expect_err("duplicate ignore attributes must fail closed");
    assert!(error.contains("tests/duplicate.rs::duplicate_case"));
    assert!(error.contains("duplicate ignore annotation"));

    let overlapping = r#"
#[test]
#[ignore = "unconditional gap"]
#[cfg_attr(unix, ignore = "conditional gap")]
fn overlapping_case() {}
"#;
    let error = collect_ignored_tests(Path::new("tests/overlap.rs"), overlapping)
        .expect_err("potentially overlapping ignore policies must fail closed");
    assert!(error.contains("tests/overlap.rs::overlapping_case"));
    assert!(error.contains("overlapping or ambiguous"));
}

#[test]
fn test_ignore_source_collector_rejects_bare_empty_and_nonliteral_reasons() {
    let cases = [
        (
            "bare",
            r"
#[test]
#[ignore]
fn malformed_case() {}
",
            "require a reason",
        ),
        (
            "empty",
            r#"
#[test]
#[ignore = "   "]
fn malformed_case() {}
"#,
            "must not be empty",
        ),
        (
            "padded",
            r#"
#[test]
#[ignore = " padded reason "]
fn malformed_case() {}
"#,
            "leading or trailing whitespace",
        ),
        (
            "nonliteral",
            r#"
#[test]
#[ignore = concat!("computed", " reason")]
fn malformed_case() {}
"#,
            "must be a string literal",
        ),
        (
            "nested_bare",
            r#"
#[test]
#[cfg_attr(all(unix, not(miri)), cfg_attr(feature = "x", ignore))]
fn malformed_case() {}
"#,
            "require a reason",
        ),
    ];

    for (case_name, source, expected) in cases {
        let path = PathBuf::from(format!("tests/{case_name}.rs"));
        let error = collect_ignored_tests(&path, source)
            .expect_err("malformed ignore policy must fail closed");
        assert!(
            error.contains(expected),
            "case={case_name} expected={expected:?} error={error:?}"
        );
    }
}

#[test]
fn test_ignore_source_collector_rejects_ignore_on_non_test_items() {
    let non_test_function = r#"
#[ignore = "not a test"]
fn helper() {}
"#;
    let error = collect_ignored_tests(Path::new("tests/non_test_fn.rs"), non_test_function)
        .expect_err("ignore on a non-test function must fail closed");
    assert!(error.contains("non_test_fn.rs::helper"));
    assert!(error.contains("ordinary `#[test]` function"));

    let non_test_item = r#"
#[ignore = "not a test"]
struct Helper;
"#;
    let error = collect_ignored_tests(Path::new("tests/non_test_item.rs"), non_test_item)
        .expect_err("ignore on a non-function item must fail closed");
    assert!(error.contains("ordinary `#[test]` function"));
}

#[test]
fn test_ignore_source_collector_fails_closed_on_ignore_bearing_macro_tokens() {
    let generated = r#"
macro_rules! generated_test {
    () => {
        #[test]
        #[ignore = "hidden by expansion"]
        fn generated_case() {}
    };
}
"#;
    let error = collect_ignored_tests(Path::new("tests/generated.rs"), generated)
        .expect_err("ignore generated by macro tokens must fail closed");
    assert!(
        error.contains("macro `macro_rules!`"),
        "unexpected collector error: {error}"
    );
    assert!(
        error.contains("cannot be audited without expansion"),
        "unexpected collector error: {error}"
    );

    let invocation = r#"
generate_case! {
    #[test]
    #[cfg_attr(all(unix, not(miri)), ignore = "hidden invocation")]
    fn generated_case() {}
}
"#;
    let error = collect_ignored_tests(Path::new("tests/invocation.rs"), invocation)
        .expect_err("nested cfg_attr ignore in macro input must fail closed");
    assert!(
        error.contains("macro `generate_case!`"),
        "unexpected collector error: {error}"
    );
    assert!(
        error.contains("cannot be audited without expansion"),
        "unexpected collector error: {error}"
    );
}

#[test]
fn test_ignore_source_collector_rejects_macro_generated_test_boundaries() {
    let cases = [
        (
            "forwarded_attribute",
            r"
macro_rules! forwarded_attribute {
    ($attr:meta) => { #[$attr] fn generated_case() {} };
}
",
            "macro-forwarded attribute",
        ),
        (
            "repeated_attributes",
            r"
macro_rules! repeated_attributes {
    ($(#[$attr:meta])*) => { $(#[$attr])* fn generated_case() {} };
}
",
            "macro-forwarded attribute",
        ),
        (
            "nested_forwarding",
            r"
macro_rules! nested_forwarding {
    ($attr:meta) => { #[cfg_attr(unix, $attr)] fn generated_case() {} };
}
",
            "macro-forwarded attribute",
        ),
        (
            "generated_test",
            r"
macro_rules! generated_test {
    () => { #[test] fn generated_case() {} };
}
",
            "test attribute",
        ),
        (
            "dynamic_callee",
            r"
macro_rules! dynamic_callee {
    ($callee:ident) => { $callee! { fn generated_case() {} } };
}
",
            "dynamically selected macro invocation",
        ),
    ];

    for (case_name, source, expected) in cases {
        let path = PathBuf::from(format!("tests/{case_name}.rs"));
        let error = collect_ignored_tests(&path, source)
            .expect_err("dynamic macro test boundaries must fail closed");
        assert!(
            error.contains(expected),
            "case={case_name} expected={expected:?} error={error:?}"
        );
    }
}

#[test]
fn test_ignore_source_collector_preserves_ordinary_non_test_macros() {
    let source = r#"
macro_rules! log_values {
    ($($arg:tt)*) => { println!($($arg)*); };
}

#[test]
fn active_case() {
    log_values!("active");
}
"#;
    let records = collect_ignored_tests(Path::new("tests/logging_macro.rs"), source)
        .expect("ordinary non-test macros must remain auditable");
    assert!(records.is_empty());
}

#[test]
fn test_ignore_source_collector_allows_active_proptest_but_rejects_ignored_proptest() {
    let active = r"
proptest! {
    #[test]
    fn generated_case(value in 0_u8..10) {
        prop_assert!(value < 10);
    }
}
";
    let records = collect_ignored_tests(Path::new("tests/active_proptest.rs"), active)
        .expect("audited active proptest boundaries should be allowed");
    assert!(records.is_empty());

    let namespaced_impostor = r"
local::proptest! {
    #[test]
    fn generated_case() {}
}
";
    let error = collect_ignored_tests(Path::new("tests/impostor_proptest.rs"), namespaced_impostor)
        .expect_err("an arbitrary namespaced macro must not inherit the proptest exception");
    assert!(error.contains("test attribute"));

    let ignored_cases = [
        r#"
proptest! {
    #[test]
    #[ignore = "hidden generated case"]
    fn generated_case(value in 0_u8..10) {
        prop_assert!(value < 10);
    }
}
"#,
        r#"
proptest! {
    #[cfg_attr(unix, test, ignore = "hidden after test")]
    fn generated_case(value in 0_u8..10) {
        prop_assert!(value < 10);
    }
}
"#,
        r#"
proptest! {
    #[cfg_attr(unix, ignore = "hidden before test", test)]
    fn generated_case(value in 0_u8..10) {
        prop_assert!(value < 10);
    }
}
"#,
        r#"
proptest! {
    #[cfg_attr(unix, test, cfg_attr(miri, ignore = "nested hidden ignore"))]
    fn generated_case(value in 0_u8..10) {
        prop_assert!(value < 10);
    }
}
"#,
    ];
    for (case_index, ignored) in ignored_cases.into_iter().enumerate() {
        let error = collect_ignored_tests(Path::new("tests/ignored_proptest.rs"), ignored)
            .expect_err("ignored proptest boundaries must fail closed");
        assert!(
            error.contains("ignore-bearing attribute"),
            "case {case_index} returned the wrong finding: {error}"
        );
    }
}

#[test]
fn test_ignore_source_collector_accepts_only_tracked_literal_rust_includes() {
    let host_path = Path::new("tests/include_host.rs");
    let target_path = Path::new("tests/fixtures/tracked.rs");
    let inventory = HashSet::from([
        normalize_source_path(host_path).expect("normalize host"),
        normalize_source_path(target_path).expect("normalize target"),
    ]);
    let host = r#"include!("fixtures/tracked.rs");"#;
    let target = r#"
#[test]
#[ignore = "tracked included test"]
fn included_case() {}
"#;

    let host_records = collect_ignored_tests_with_inventory(host_path, host, &inventory)
        .expect("tracked literal Rust include should be accepted");
    assert!(host_records.is_empty());
    let qualified_host = r#"std::include!("fixtures/tracked.rs");"#;
    let host_records = collect_ignored_tests_with_inventory(host_path, qualified_host, &inventory)
        .expect("qualified tracked literal Rust include should be accepted");
    assert!(host_records.is_empty());
    let target_records = collect_ignored_tests_with_inventory(target_path, target, &inventory)
        .expect("included source is inventoried independently");
    assert_eq!(target_records.len(), 1);
    assert_eq!(target_records[0].test_name, "included_case");
}

#[test]
fn test_ignore_source_collector_rejects_uncontrolled_includes() {
    let cases = [
        (
            "computed",
            r#"include!(concat!(env!("OUT_DIR"), "/generated.rs"));"#,
            "must use one tracked repository-relative Rust string literal",
        ),
        (
            "qualified_computed",
            r#"std::include!(concat!(env!("OUT_DIR"), "/generated.rs"));"#,
            "must use one tracked repository-relative Rust string literal",
        ),
        (
            "untracked",
            r#"include!("fixtures/missing.rs");"#,
            "not tracked by the source inventory",
        ),
        (
            "non_rust",
            r#"include!("fixtures/data.txt");"#,
            "must be a `.rs` file",
        ),
        (
            "absolute",
            r#"include!("/tmp/generated.rs");"#,
            "repository-relative",
        ),
        (
            "escaping",
            r#"include!("../../outside.rs");"#,
            "escapes the repository root",
        ),
        (
            "nested",
            r#"
outer! {
    include!("fixtures/missing.rs");
}
"#,
            "not tracked by the source inventory",
        ),
    ];

    for (case_name, source, expected) in cases {
        let path = PathBuf::from(format!("tests/{case_name}.rs"));
        let mut inventory =
            HashSet::from([normalize_source_path(&path).expect("normalize include host")]);
        if case_name == "non_rust" {
            inventory.insert("tests/fixtures/data.txt".to_owned());
        }
        let error = collect_ignored_tests_with_inventory(&path, source, &inventory)
            .expect_err("uncontrolled include boundaries must fail closed");
        assert!(
            error.contains(expected),
            "case={case_name} expected={expected:?} error={error:?}"
        );
    }
}

#[test]
fn test_ignore_source_collector_exact_uninspected_boundary_never_reads_the_source() {
    let source_paths = vec![
        "tests/z.rs".to_owned(),
        "tests/opaque_extra.rs".to_owned(),
        "tests/opaque.rs".to_owned(),
        "tests/a.rs".to_owned(),
    ];
    let mut reads = Vec::new();
    let inventory = collect_repository_ignored_tests_from_paths_with_reader(
        &source_paths,
        &["tests/opaque.rs"],
        |source_path| {
            assert_ne!(
                source_path, "tests/opaque.rs",
                "the exact uninspected source must never reach the reader"
            );
            reads.push(source_path.to_owned());
            if source_path == "tests/opaque_extra.rs" {
                Ok("#[test]\n#[ignore = \"near-prefix gap\"]\nfn near_prefix() {}\n".to_owned())
            } else if source_path == "tests/z.rs" {
                Ok("#[test]\n#[ignore = \"z gap\"]\nfn z_case() {}\n".to_owned())
            } else {
                Ok("#[test]\nfn active_case() {}\n".to_owned())
            }
        },
    )
    .expect("exact uninspected boundary should preserve the inspected inventory");

    assert_eq!(inventory.uninspected_sources, ["tests/opaque.rs"]);
    assert_eq!(reads, ["tests/a.rs", "tests/opaque_extra.rs", "tests/z.rs"]);
    assert_eq!(
        inventory
            .records
            .iter()
            .map(IgnoredTestSource::locator)
            .collect::<Vec<_>>(),
        ["tests/opaque_extra.rs::near_prefix", "tests/z.rs::z_case"]
    );
}

#[test]
fn test_ignore_source_collector_rejects_invalid_uninspected_boundaries() {
    let empty_reader = |_: &str| Ok::<String, String>(String::new());

    let duplicate_sources = vec!["tests/a.rs".to_owned(), "tests/a.rs".to_owned()];
    assert!(
        collect_repository_ignored_tests_from_paths_with_reader(
            &duplicate_sources,
            &[],
            empty_reader,
        )
        .expect_err("duplicate tracked paths must fail")
        .contains("duplicate path")
    );

    let noncanonical_sources = vec!["./tests/a.rs".to_owned()];
    assert!(
        collect_repository_ignored_tests_from_paths_with_reader(
            &noncanonical_sources,
            &[],
            empty_reader,
        )
        .expect_err("noncanonical tracked paths must fail")
        .contains("not canonically spelled")
    );

    let tracked_sources = vec!["tests/a.rs".to_owned()];
    assert!(
        collect_repository_ignored_tests_from_paths_with_reader(
            &tracked_sources,
            &["tests/missing.rs"],
            empty_reader,
        )
        .expect_err("untracked opaque paths must fail")
        .contains("not tracked")
    );
    assert!(
        collect_repository_ignored_tests_from_paths_with_reader(
            &tracked_sources,
            &["tests/a.rs", "tests/a.rs"],
            empty_reader,
        )
        .expect_err("duplicate opaque paths must fail")
        .contains("duplicate uninspected")
    );
}

#[test]
fn test_ignore_source_collector_reports_scoped_repository_inventory() {
    let root = repo_root();
    let inventory = collect_repository_ignored_tests(&root, UNINSPECTED_RUST_SOURCE_PATHS)
        .expect("every inspected tracked Rust source must be syntax-auditable");
    assert_eq!(
        inventory.uninspected_sources, UNINSPECTED_RUST_SOURCE_PATHS,
        "the exact uninspected boundary must remain visible and deterministic"
    );
    assert_eq!(
        inventory.soundness_limitations, SOURCE_INVENTORY_SOUNDNESS_LIMITATIONS,
        "known collector soundness limits must remain explicit release blockers"
    );
    assert!(
        !inventory.records.is_empty(),
        "repository source inventory unexpectedly found no ignored tests"
    );
    assert!(
        inventory
            .records
            .windows(2)
            .all(|pair| pair[0].locator() < pair[1].locator()),
        "repository ignored-test locators must be strictly ordered and unique"
    );
    let baseline = parse_regression_baseline(&baseline_path(&root))
        .expect("tracked ignored-test taxonomy must be schema-valid");
    let mismatches = compare_ignored_test_taxonomy(&baseline.ignored_tests, &inventory.records);
    assert!(
        mismatches.is_empty(),
        "inspected ignored-test taxonomy drifted: {mismatches:#?}"
    );
}

#[test]
fn test_regression_guard_parses_cargo_output() {
    let sample = r"
     Running unittests src/lib.rs (target/debug/deps/example-a1)
test result: ok. 4 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out; finished in 0.00s
     Running tests/integration.rs (target/debug/deps/integration-b2)
test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
";

    let counts = parse_workspace_test_counts(sample)
        .expect("sample output should parse into aggregate regression counts");
    assert_eq!(
        counts.total_tests, 7,
        "bead_id={BEAD_ID} case=parse_output_total"
    );
    assert_eq!(
        counts.passed, 6,
        "bead_id={BEAD_ID} case=parse_output_passed"
    );
    assert_eq!(
        counts.failed, 0,
        "bead_id={BEAD_ID} case=parse_output_failed"
    );
    assert_eq!(
        counts.ignored, 1,
        "bead_id={BEAD_ID} case=parse_output_ignored"
    );
}

#[test]
fn test_regression_guard_uses_last_summary_per_target_section() {
    let sample = r"
     Running tests/parent.rs (target/debug/deps/parent-a1)
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
test result: ok. 4 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out; finished in 0.00s
   Doc-tests example
test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
";

    let counts = parse_workspace_test_counts(sample)
        .expect("last summary in each target section should be authoritative");
    assert_eq!(counts.total_tests, 7);
    assert_eq!(counts.passed, 6);
    assert_eq!(counts.failed, 0);
    assert_eq!(counts.ignored, 1);
}

#[test]
fn test_regression_guard_rejects_unframed_malformed_and_colored_summaries() {
    let unframed = "test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out";
    assert!(
        parse_workspace_test_counts(unframed)
            .expect_err("summary without target section must fail")
            .contains("outside a target section")
    );

    let malformed = "     Running tests/example.rs (example)\ntest result: ok. not-a-count passed";
    assert!(
        parse_workspace_test_counts(malformed)
            .expect_err("malformed summary must fail")
            .contains("malformed cargo test summary")
    );

    let missing = "     Running tests/example.rs (example)";
    assert!(
        parse_workspace_test_counts(missing)
            .expect_err("missing summary must fail")
            .contains("had no test-result summary")
    );

    let colored = "\u{1b}[32mRunning tests/example.rs (example)\u{1b}[0m";
    assert!(
        parse_workspace_test_counts(colored)
            .expect_err("colored transcript must fail closed")
            .contains("ANSI escape")
    );

    let invalid_outcome = "     Running tests/example.rs (example)\ntest result: MAYBE. 1 passed; 0 failed; 0 ignored";
    assert!(
        parse_workspace_test_counts(invalid_outcome)
            .expect_err("unknown libtest outcome must fail")
            .contains("malformed cargo test summary")
    );
}

#[test]
fn test_regression_guard_ignores_unanchored_subprocess_noise() {
    let sample = r"
     Running tests/outer.rs (target/debug/deps/outer-a1)
helper: test result: ok. 90 passed; 0 failed; 0 ignored
  Running tests/not-a-cargo-header.rs (helper)
test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
";

    let counts = parse_workspace_test_counts(sample)
        .expect("unanchored subprocess output must not alter Cargo target counts");
    assert_eq!(counts.total_tests, 2);
    assert_eq!(counts.passed, 2);
    assert_eq!(counts.failed, 0);
    assert_eq!(counts.ignored, 0);
}

#[test]
fn test_regression_guard_exact_test_transcript_rejects_zero_or_wrong_tests() {
    let mut entry = sample_ignored_baseline("crates/example/tests/case.rs", "exact_case");
    entry.kind = IgnoreKind::Stress;
    entry.policy = IgnorePolicy::RunForRelease;
    let passing = r"
     Running tests/case.rs (target/debug/deps/case-a1)
test exact_case ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
";
    assert_eq!(validate_single_test_transcript(passing, &entry), Ok(()));

    let zero = r"
     Running tests/case.rs (target/debug/deps/case-a1)
test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 1 filtered out; finished in 0.00s
";
    assert!(
        validate_single_test_transcript(zero, &entry)
            .expect_err("zero selected tests must fail")
            .contains("exactly one selected")
    );
    let mut different_test = entry.clone();
    different_test.test_name = "different_case".to_owned();
    assert!(
        validate_single_test_transcript(passing, &different_test)
            .expect_err("a different passing test must not satisfy the receipt")
            .contains("matching success line")
    );

    let wrong_target = passing.replace("tests/case.rs", "tests/other.rs");
    assert!(
        validate_single_test_transcript(&wrong_target, &entry)
            .expect_err("a same-named test from another target must fail")
            .contains("canonical Cargo target")
    );
}

#[test]
fn test_regression_guard_release_perf_command_matches_conditional_ignore() {
    let mut entry = sample_ignored_baseline(
        "crates/fsqlite-e2e/tests/manual_release.rs",
        "manual_release_case",
    );
    entry.kind = IgnoreKind::Performance;
    entry.policy = IgnorePolicy::RunForRelease;

    let unconditional = expected_current_run_argv(&entry)
        .expect("an unconditional ignore has a canonical release command");
    assert_eq!(
        unconditional,
        [
            "cargo",
            "test",
            "--locked",
            "--profile",
            "release-perf",
            "--package",
            "fsqlite-e2e",
            "--test",
            "manual_release",
            "manual_release_case",
            "--",
            "--exact",
            "--ignored",
            "--nocapture",
            "--test-threads=1",
        ]
    );

    entry.cfg_condition = Some("debug_assertions".to_owned());
    let debug_only = expected_current_run_argv(&entry)
        .expect("release-perf disables a debug-assertions-only ignore");
    assert_eq!(
        debug_only,
        [
            "cargo",
            "test",
            "--locked",
            "--profile",
            "release-perf",
            "--package",
            "fsqlite-e2e",
            "--test",
            "manual_release",
            "manual_release_case",
            "--",
            "--exact",
            "--nocapture",
            "--test-threads=1",
        ]
    );

    entry.cfg_condition = Some("all(debug_assertions,test)".to_owned());
    assert_eq!(
        expected_current_run_argv(&entry)
            .expect("test-module debug-only ignores use the same release contract"),
        debug_only
    );

    entry.cfg_condition = Some("unix".to_owned());
    assert!(
        expected_current_run_argv(&entry)
            .expect_err("unmodeled conditional ignores must fail closed")
            .contains("typed compilation contract")
    );
}

#[test]
fn test_regression_guard_release_manifest_requires_complete_typed_receipts() {
    let (baseline, manifest) = sample_release_evidence();
    let validated = validate_release_evidence_manifest(&manifest, &baseline)
        .expect("complete canonical evidence must validate structurally");
    assert!(validated.contains("crates/fsqlite-e2e/tests/manual_release.rs::manual_release_case"));
    assert!(!validated.contains(RELEASE_GUARD_LOCATOR));

    let mut missing = manifest.clone();
    missing.run_receipts.clear();
    assert!(
        validate_release_evidence_manifest(&missing, &baseline)
            .expect_err("missing run_for_release evidence must fail")
            .contains("missing=")
    );

    let mut wrong_requirement = manifest.clone();
    wrong_requirement.run_receipts[0].requirement_blake3 = "c".repeat(64);
    assert!(
        validate_release_evidence_manifest(&wrong_requirement, &baseline)
            .expect_err("stale requirement evidence must fail")
            .contains("current evidence requirement")
    );

    let mut wrong_command = manifest.clone();
    wrong_command.run_receipts[0]
        .execution
        .argv
        .push("--nocapture".to_owned());
    assert!(
        validate_release_evidence_manifest(&wrong_command, &baseline)
            .expect_err("noncanonical command evidence must fail")
            .contains("canonical exact-test command")
    );

    let mut failed_command = manifest.clone();
    failed_command.run_receipts[0].execution.exit_status = 1;
    assert!(
        validate_release_evidence_manifest(&failed_command, &baseline)
            .expect_err("failed command evidence must fail")
            .contains("zero command and capture statuses")
    );

    let mut conditional_baseline = baseline.clone();
    conditional_baseline.ignored_tests[0].cfg_condition = Some("unix".to_owned());
    assert!(
        validate_release_evidence_manifest(&manifest, &conditional_baseline)
            .expect_err("cfg-dependent evidence without a typed build contract must fail")
            .contains("typed compilation contract")
    );

    let mut wrong_guard_kind = baseline.clone();
    wrong_guard_kind
        .ignored_tests
        .iter_mut()
        .find(|entry| entry.locator() == RELEASE_GUARD_LOCATOR)
        .expect("sample baseline contains live guard")
        .kind = IgnoreKind::Performance;
    assert!(
        validate_release_evidence_manifest(&manifest, &wrong_guard_kind)
            .expect_err("live guard kind drift must fail")
            .contains("release_gate kind")
    );
}

#[test]
fn test_regression_guard_cargo_target_mapping_rejects_manifest_overrides() {
    let mut library_entry = sample_ignored_baseline("crates/example/src/lib.rs", "library_case");
    library_entry.kind = IgnoreKind::Stress;
    library_entry.policy = IgnorePolicy::RunForRelease;
    let custom_library = toml::from_str::<toml::Table>(
        r#"
            [package]
            name = "example"
            [lib]
            path = "src/custom.rs"
        "#,
    )
    .expect("parse custom library manifest");
    assert!(
        validate_cargo_manifest_target_contract(&custom_library, "example", &library_entry)
            .expect_err("custom library paths require a typed target contract")
            .contains("noncanonical Cargo override")
    );
    let custom_harness = toml::from_str::<toml::Table>(
        r#"
            [package]
            name = "example"
            [lib]
            harness = false
        "#,
    )
    .expect("parse custom harness manifest");
    assert!(
        validate_cargo_manifest_target_contract(&custom_harness, "example", &library_entry)
            .expect_err("custom library harnesses require a typed target contract")
            .contains("noncanonical Cargo override")
    );

    let mut integration_entry =
        sample_ignored_baseline("crates/example/tests/case.rs", "integration_case");
    integration_entry.kind = IgnoreKind::Stress;
    integration_entry.policy = IgnorePolicy::RunForRelease;
    let explicit_test = toml::from_str::<toml::Table>(
        r#"
            [package]
            name = "example"
            [[test]]
            name = "renamed_case"
            path = "./tests/case.rs"
            required-features = ["special"]
        "#,
    )
    .expect("parse explicit test manifest");
    assert!(
        validate_cargo_manifest_target_contract(&explicit_test, "example", &integration_entry)
            .expect_err("explicit test targets require a typed target contract")
            .contains("explicit Cargo override")
    );
}

#[test]
fn test_regression_guard_release_manifest_rejects_circular_or_ambiguous_evidence() {
    let (baseline, manifest) = sample_release_evidence();

    let mut circular = manifest.clone();
    let guard = baseline
        .ignored_tests
        .iter()
        .find(|entry| entry.locator() == RELEASE_GUARD_LOCATOR)
        .expect("sample baseline contains live guard");
    circular.run_receipts.push(CurrentRunReceipt {
        source_path: guard.source_path.clone(),
        test_name: guard.test_name.clone(),
        requirement_blake3: blake3::hash(guard.evidence.requirement.as_bytes())
            .to_hex()
            .to_string(),
        execution: sample_command_evidence(
            expected_current_run_argv(guard).expect("guard source has a canonical target"),
            "tests/artifacts/release-evidence/live-guard.txt",
        ),
    });
    assert!(
        validate_release_evidence_manifest(&circular, &baseline)
            .expect_err("external live-guard evidence must fail")
            .contains("circular external receipt")
    );

    let mut duplicate_artifact = manifest.clone();
    duplicate_artifact.run_receipts[0].execution.artifact_path =
        duplicate_artifact.workspace.artifact_path.clone();
    assert!(
        validate_release_evidence_manifest(&duplicate_artifact, &baseline)
            .expect_err("reused evidence artifacts must fail")
            .contains("artifact paths must be unique")
    );

    let unknown_field = r#"{
        "schema_version": 1,
        "tested_commit": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "workspace": {
            "argv": ["cargo"],
            "exit_status": 0,
            "capture_status": 0,
            "artifact_path": "tests/artifacts/release-evidence/workspace.txt",
            "artifact_blake3": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        },
        "run_receipts": [],
        "unexpected": true
    }"#;
    assert!(
        serde_json::from_str::<ReleaseEvidenceManifest>(unknown_field)
            .expect_err("unknown manifest fields must fail closed")
            .to_string()
            .contains("unknown field `unexpected`")
    );
}

#[test]
fn test_regression_guard_live_self_receipt_requires_exact_ignored_invocation() {
    let test_name = "phase5_regression_guard_full_workspace_against_baseline".to_owned();
    let exact = vec![
        "test-binary".to_owned(),
        test_name.clone(),
        "--exact".to_owned(),
        "--ignored".to_owned(),
    ];
    assert_eq!(validate_live_release_guard_invocation(&exact), Ok(()));

    for invalid in [
        vec!["test-binary".to_owned(), test_name.clone()],
        vec![
            "test-binary".to_owned(),
            test_name.clone(),
            "--exact".to_owned(),
        ],
        vec![
            "test-binary".to_owned(),
            test_name,
            "--exact".to_owned(),
            "--ignored".to_owned(),
            "--skip".to_owned(),
        ],
    ] {
        assert!(validate_live_release_guard_invocation(&invalid).is_err());
    }
}

#[test]
fn test_regression_guard_baseline_schema_rejects_unknown_fields() {
    let baseline = r#"{
        "as_of_phase": "checkpoint_1",
        "total_tests": 1,
        "passed": 1,
        "failed": 0,
        "ignored": 0,
        "baseline_commit": "deadbeef",
        "ignored_tests": [],
        "unexpected": true
    }"#;
    let error = serde_json::from_str::<RegressionBaseline>(baseline)
        .expect_err("unknown baseline fields must fail closed");
    assert!(error.to_string().contains("unknown field `unexpected`"));
}

#[test]
fn test_regression_guard_tracked_baseline_is_valid() {
    let root = repo_root();
    let baseline = parse_regression_baseline(&baseline_path(&root))
        .expect("tracked regression baseline must be schema-valid");
    assert!(
        !baseline.ignored_tests.is_empty(),
        "tracked regression baseline must inventory reasoned ignores"
    );
}

#[test]
fn test_regression_guard_release_loader_verifies_git_provenance_and_rename_delta() {
    let repository = tempfile::tempdir().expect("create isolated Git repository");
    let root = repository.path();
    let git = |arguments: &[&str]| {
        Command::new("git")
            .arg("-C")
            .arg(root)
            .args(arguments)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .expect("run Git command")
    };
    assert!(git(&["init", "--initial-branch=main"]).success());
    assert!(git(&["config", "user.name", "Regression Guard Keeper"]).success());
    assert!(git(&["config", "user.email", "guard@example.invalid"]).success());
    assert!(git(&["config", "commit.gpgsign", "false"]).success());
    assert!(git(&["config", "core.autocrlf", "false"]).success());
    assert!(git(&["config", "core.hooksPath", ".no-hooks"]).success());
    fs::write(root.join("seed.txt"), "seed\n").expect("write seed");
    fs::write(root.join("source.rs"), "fn source() {}\n").expect("write source");
    fs::write(
        root.join(".gitignore"),
        format!("/{REGRESSION_BASELINE_PATH}\n"),
    )
    .expect("write ignore rule");
    assert!(git(&["add", "--force", ".gitignore", "seed.txt", "source.rs",]).success());
    assert!(git(&["commit", "-m", "seed"]).success());
    let baseline_commit = resolve_current_head(root).expect("resolve isolated HEAD");
    let baseline_path = root.join(REGRESSION_BASELINE_PATH);
    fs::create_dir_all(baseline_path.parent().expect("baseline has parent"))
        .expect("create baseline directory");
    fs::write(
        &baseline_path,
        format!(
            r#"{{
                "as_of_phase": "checkpoint_1",
                "total_tests": 1,
                "passed": 1,
                "failed": 0,
                "ignored": 0,
                "baseline_commit": "{baseline_commit}",
                "ignored_tests": []
            }}"#
        ),
    )
    .expect("write valid baseline");
    assert!(
        load_regression_baseline(&baseline_path, root, &baseline_commit)
            .expect_err("ignored untracked baseline must fail")
            .contains("tracked by Git")
    );
    assert!(
        git(&["add", "--force", REGRESSION_BASELINE_PATH]).success(),
        "force-add the intentionally ignored keeper baseline"
    );
    assert!(git(&["commit", "-m", "add baseline"]).success());
    let baseline_head = resolve_current_head(root).expect("resolve baseline HEAD");
    assert!(load_regression_baseline(&baseline_path, root, &baseline_head).is_ok());

    let missing_commit = "f".repeat(40);
    fs::write(
        &baseline_path,
        format!(
            r#"{{
                "as_of_phase": "checkpoint_1",
                "total_tests": 1,
                "passed": 1,
                "failed": 0,
                "ignored": 0,
                "baseline_commit": "{missing_commit}",
                "ignored_tests": []
            }}"#
        ),
    )
    .expect("write missing-commit baseline");
    assert!(git(&["add", "--force", REGRESSION_BASELINE_PATH]).success());
    assert!(git(&["commit", "-m", "use missing baseline commit"]).success());
    let missing_head = resolve_current_head(root).expect("resolve missing-commit HEAD");
    assert!(
        load_regression_baseline(&baseline_path, root, &missing_head)
            .expect_err("missing baseline commit must fail")
            .contains("does not resolve")
    );

    let evidence_directory = root.join(EVIDENCE_PATH_PREFIX);
    fs::create_dir_all(&evidence_directory).expect("create evidence directory");
    fs::rename(
        root.join("source.rs"),
        evidence_directory.join("renamed.txt"),
    )
    .expect("rename source into evidence directory");
    assert!(git(&["add", "--all"]).success());
    assert!(git(&["commit", "-m", "rename source as evidence"]).success());
    let renamed_head = resolve_current_head(root).expect("resolve renamed HEAD");
    let changed = changed_paths_between(root, &missing_head, &renamed_head)
        .expect("rename delta must remain inspectable");
    assert!(changed.contains("source.rs"));
    assert!(changed.contains("tests/artifacts/release-evidence/renamed.txt"));
}

#[test]
fn test_regression_guard_release_manifest_loader_is_commit_and_content_bound() {
    let repository = tempfile::tempdir().expect("create isolated evidence repository");
    let root = repository.path();
    let git = |arguments: &[&str]| {
        Command::new("git")
            .arg("-C")
            .arg(root)
            .args(arguments)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .expect("run Git command")
    };
    assert!(git(&["init", "--initial-branch=main"]).success());
    assert!(git(&["config", "user.name", "Release Evidence Keeper"]).success());
    assert!(git(&["config", "user.email", "evidence@example.invalid"]).success());
    assert!(git(&["config", "commit.gpgsign", "false"]).success());
    assert!(git(&["config", "core.autocrlf", "false"]).success());
    assert!(git(&["config", "core.hooksPath", ".no-hooks"]).success());
    fs::write(root.join("seed.txt"), "tested tree\n").expect("write tested tree");
    assert!(git(&["add", "--force", "seed.txt"]).success());
    assert!(git(&["commit", "-m", "tested tree"]).success());
    let tested_commit = resolve_current_head(root).expect("resolve tested commit");

    let mut live_guard = sample_ignored_baseline(
        "crates/fsqlite-harness/tests/phase5_regression_guard.rs",
        "phase5_regression_guard_full_workspace_against_baseline",
    );
    live_guard.kind = IgnoreKind::ReleaseGate;
    live_guard.policy = IgnorePolicy::RunForRelease;
    let baseline = RegressionBaseline {
        as_of_phase: "checkpoint_1".to_owned(),
        total_tests: 1,
        passed: 1,
        failed: 0,
        ignored: 0,
        baseline_commit: tested_commit.clone(),
        ignored_tests: vec![live_guard],
    };

    let evidence_directory = root.join(EVIDENCE_PATH_PREFIX);
    fs::create_dir_all(&evidence_directory).expect("create evidence directory");
    let workspace_path = format!("{EVIDENCE_PATH_PREFIX}workspace.txt");
    let workspace = concat!(
        "     Running unittests src/lib.rs (target/release-perf/deps/example-a1)\n",
        "test exact_case ... ok\n",
        "test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s\n"
    );
    fs::write(root.join(&workspace_path), workspace).expect("write workspace transcript");
    let workspace_digest = blake3::hash(workspace.as_bytes()).to_hex().to_string();
    let manifest_path = format!("{EVIDENCE_PATH_PREFIX}manifest.json");
    let manifest = serde_json::json!({
        "schema_version": EVIDENCE_SCHEMA_VERSION,
        "tested_commit": tested_commit.clone(),
        "workspace": {
            "argv": CANONICAL_WORKSPACE_TEST_ARGV,
            "exit_status": 0,
            "capture_status": 0,
            "artifact_path": workspace_path.clone(),
            "artifact_blake3": workspace_digest.clone(),
        },
        "run_receipts": [],
    });
    let manifest_bytes = serde_json::to_vec_pretty(&manifest).expect("serialize manifest");
    let manifest_digest = blake3::hash(&manifest_bytes).to_hex().to_string();
    fs::write(root.join(&manifest_path), manifest_bytes).expect("write evidence manifest");
    assert!(git(&["add", "--force", "tests/artifacts/release-evidence",]).success());
    assert!(git(&["commit", "-m", "release evidence"]).success());
    let evidence_head = resolve_current_head(root).expect("resolve evidence commit");

    let evidence = load_release_evidence_manifest_from_path(
        root,
        &evidence_head,
        &baseline,
        &manifest_path,
        &manifest_digest,
    )
    .expect("commit-bound evidence manifest must validate");
    assert_eq!(evidence.tested_commit, baseline.baseline_commit);
    assert_eq!(evidence.workspace_counts.total_tests, 1);
    assert!(evidence.current_run_receipts.locators.is_empty());

    fs::write(root.join(&workspace_path), "tampered\n").expect("tamper transcript");
    assert!(
        load_release_evidence_manifest_from_path(
            root,
            &evidence_head,
            &baseline,
            &manifest_path,
            &manifest_digest,
        )
        .expect_err("dirty evidence must fail")
        .contains("pristine checkout")
    );
    assert!(git(&["add", &workspace_path]).success());
    assert!(git(&["commit", "-m", "tamper evidence"]).success());
    let tampered_head = resolve_current_head(root).expect("resolve tampered evidence commit");
    assert!(
        load_release_evidence_manifest_from_path(
            root,
            &tampered_head,
            &baseline,
            &manifest_path,
            &manifest_digest,
        )
        .expect_err("hash-mismatched evidence must fail")
        .contains("content hash mismatch")
    );
}

#[cfg(unix)]
#[test]
fn test_regression_guard_release_inputs_reject_symlinks() {
    use std::os::unix::fs::symlink;

    let directory = tempfile::tempdir().expect("create symlink keeper directory");
    fs::write(directory.path().join("outside.json"), "{}\n").expect("write symlink target");
    symlink("outside.json", directory.path().join("baseline.json"))
        .expect("create baseline symlink");
    assert!(
        require_regular_non_symlink_path(directory.path(), "baseline.json", "baseline")
            .expect_err("symlinked release input must fail")
            .contains("regular non-symlink")
    );
}

#[test]
fn test_regression_guard_pristine_checkout_allows_only_target_output() {
    assert!(release_status_record_is_allowed(b"!! target/"));
    assert!(release_status_record_is_allowed(
        b"!! target/release-perf/deps/"
    ));
    assert!(!release_status_record_is_allowed(b"!! .env"));
    assert!(!release_status_record_is_allowed(b"?? local-fixture.db"));
    assert!(!release_status_record_is_allowed(b" M Cargo.lock"));
}

#[test]
fn test_regression_guard_ignore_taxonomy_schema_is_closed() {
    let unknown_kind = r#"{
        "source_path": "tests/case.rs",
        "test_name": "case",
        "reason": "tracked gap",
        "cfg_condition": null,
        "kind": "mystery",
        "policy": "block_release",
        "evidence": { "requirement": "exact proof", "receipt": null }
    }"#;
    let error = serde_json::from_str::<IgnoredTestBaseline>(unknown_kind)
        .expect_err("unknown taxonomy kind must fail closed");
    assert!(error.to_string().contains("unknown variant `mystery`"));

    let unknown_evidence_field = r#"{
        "source_path": "tests/case.rs",
        "test_name": "case",
        "reason": "tracked gap",
        "cfg_condition": null,
        "kind": "known_bug",
        "policy": "block_release",
        "evidence": {
            "requirement": "exact proof",
            "receipt": null,
            "unexpected": true
        }
    }"#;
    let error = serde_json::from_str::<IgnoredTestBaseline>(unknown_evidence_field)
        .expect_err("unknown nested evidence fields must fail closed");
    assert!(error.to_string().contains("unknown field `unexpected`"));
}

#[test]
fn test_regression_guard_ignore_taxonomy_entry_validation_fails_closed() {
    let valid = sample_ignored_baseline("tests/case.rs", "case");
    assert_eq!(valid.validate(), Ok(()));

    let mut noncanonical_path = valid.clone();
    noncanonical_path.source_path = "./tests/case.rs".to_owned();
    assert!(
        noncanonical_path
            .validate()
            .expect_err("noncanonical source path must fail")
            .contains("not canonically spelled")
    );

    let mut noncanonical_cfg = valid.clone();
    noncanonical_cfg.cfg_condition = Some("all(unix,feature=\"a\")".to_owned());
    assert!(
        noncanonical_cfg
            .validate()
            .expect_err("noncanonical cfg condition must fail")
            .contains("cfg_condition is not canonical")
    );

    let mut blank_requirement = valid.clone();
    blank_requirement.evidence.requirement = "  ".to_owned();
    assert!(
        blank_requirement
            .validate()
            .expect_err("blank evidence requirement must fail")
            .contains("evidence.requirement")
    );

    let mut blank_receipt = valid.clone();
    let mut malformed_receipt = sample_ignore_receipt(None);
    malformed_receipt.artifact_blake3 = " ".to_owned();
    blank_receipt.evidence.receipt = Some(malformed_receipt);
    assert!(
        blank_receipt
            .validate()
            .expect_err("malformed evidence receipt must fail")
            .contains("artifact_blake3")
    );

    let mut uncovered_parent = valid;
    uncovered_parent.policy = IgnorePolicy::CoveredByParent;
    assert!(
        uncovered_parent
            .validate()
            .expect_err("known bugs cannot delegate to a parent test")
            .contains("known_bug")
    );

    let mut invalid_placeholder =
        sample_ignored_baseline("tests/placeholder.rs", "placeholder_case");
    invalid_placeholder.kind = IgnoreKind::Placeholder;
    invalid_placeholder.policy = IgnorePolicy::Exempt;
    assert!(
        invalid_placeholder
            .validate()
            .expect_err("placeholders cannot be exempt")
            .contains("placeholder")
    );

    let mut invalid_helper = sample_ignored_baseline("tests/helper.rs", "child");
    invalid_helper.kind = IgnoreKind::SubprocessHelper;
    invalid_helper.policy = IgnorePolicy::RunForRelease;
    assert!(
        invalid_helper
            .validate()
            .expect_err("subprocess helpers require parent coverage")
            .contains("subprocess_helper")
    );
}

#[test]
fn test_regression_guard_ignore_taxonomy_kind_policy_matrix_is_closed() {
    let policies = [
        IgnorePolicy::BlockRelease,
        IgnorePolicy::RunForRelease,
        IgnorePolicy::CoveredByParent,
        IgnorePolicy::Exempt,
    ];
    let cases: &[(IgnoreKind, &[IgnorePolicy])] = &[
        (IgnoreKind::KnownBug, &[IgnorePolicy::BlockRelease]),
        (IgnoreKind::Placeholder, &[IgnorePolicy::BlockRelease]),
        (
            IgnoreKind::Performance,
            &[IgnorePolicy::RunForRelease, IgnorePolicy::Exempt],
        ),
        (IgnoreKind::Stress, &[IgnorePolicy::RunForRelease]),
        (IgnoreKind::Diagnostic, &[IgnorePolicy::Exempt]),
        (
            IgnoreKind::SubprocessHelper,
            &[IgnorePolicy::CoveredByParent],
        ),
        (IgnoreKind::ArtifactGeneration, &[IgnorePolicy::Exempt]),
        (IgnoreKind::EnvironmentSpecific, &[IgnorePolicy::Exempt]),
        (
            IgnoreKind::ReleaseGate,
            &[IgnorePolicy::BlockRelease, IgnorePolicy::RunForRelease],
        ),
    ];

    for (kind, allowed) in cases {
        for policy in policies {
            assert_eq!(
                kind.allows_policy(policy),
                allowed.contains(&policy),
                "kind={} policy={}",
                kind.as_str(),
                policy.as_str()
            );
        }
    }
}

#[test]
fn test_regression_guard_ignore_taxonomy_requires_sorted_unique_locators() {
    let baseline_with = |ignored_tests| RegressionBaseline {
        as_of_phase: "checkpoint_1".to_owned(),
        total_tests: 1,
        passed: 1,
        failed: 0,
        ignored: 0,
        baseline_commit: "deadbeef".to_owned(),
        ignored_tests,
    };

    let duplicate = sample_ignored_baseline("tests/a.rs", "case");
    let error = baseline_with(vec![duplicate.clone(), duplicate])
        .validate()
        .expect_err("duplicate taxonomy locators must fail");
    assert!(error.contains("strictly sorted and unique"));

    let error = baseline_with(vec![
        sample_ignored_baseline("tests/z.rs", "case"),
        sample_ignored_baseline("tests/a.rs", "case"),
    ])
    .validate()
    .expect_err("out-of-order taxonomy locators must fail");
    assert!(error.contains("strictly sorted and unique"));
}

#[test]
fn test_regression_guard_ignore_taxonomy_reports_exact_drift_deterministically() {
    let mut expected_cfg = sample_ignored_baseline("tests/b.rs", "case");
    expected_cfg.cfg_condition = Some("unix".to_owned());
    let expected = vec![
        sample_ignored_baseline("tests/a.rs", "case"),
        expected_cfg,
        sample_ignored_baseline("tests/c.rs", "case"),
    ];

    let actual = vec![
        IgnoredTestSource {
            source_path: "tests/d.rs".to_owned(),
            test_name: "case".to_owned(),
            reason: "new gap".to_owned(),
            cfg_condition: None,
        },
        IgnoredTestSource {
            source_path: "tests/b.rs".to_owned(),
            test_name: "case".to_owned(),
            reason: "tracked gap".to_owned(),
            cfg_condition: None,
        },
        IgnoredTestSource {
            source_path: "tests/a.rs".to_owned(),
            test_name: "case".to_owned(),
            reason: "changed reason".to_owned(),
            cfg_condition: None,
        },
    ];

    assert_eq!(
        compare_ignored_test_taxonomy(&expected, &actual),
        vec![
            "changed tests/a.rs::case: reason expected=\"tracked gap\" actual=\"changed reason\"",
            "changed tests/b.rs::case: cfg_condition expected=Some(\"unix\") actual=None",
            "missing tests/c.rs::case: baseline entry has no matching source ignore",
            "unclassified tests/d.rs::case: reason=\"new gap\" cfg_condition=None",
        ]
    );
}

#[test]
fn test_regression_guard_block_release_ignores_static_and_current_receipts() {
    let mut blocker = sample_ignored_baseline("tests/blocker.rs", "case");
    blocker.evidence.receipt = Some(sample_ignore_receipt(None));
    let receipts = ValidatedCurrentRunReceipts::from_test_locators([blocker.locator()]);
    let blockers = ignored_test_release_blockers(&[blocker], &receipts, false);
    assert_eq!(blockers.len(), 1);
    assert!(blockers[0].contains("block_release policy remains unresolved"));

    let mut release_run = sample_ignored_baseline("tests/manual.rs", "case");
    release_run.kind = IgnoreKind::Stress;
    release_run.policy = IgnorePolicy::RunForRelease;
    release_run.evidence.receipt = Some(sample_ignore_receipt(None));
    assert_eq!(
        ignored_test_release_blockers(
            &[release_run.clone()],
            &ValidatedCurrentRunReceipts::default(),
            false,
        )
        .len(),
        1
    );
    assert!(
        ignored_test_release_blockers(
            &[release_run.clone()],
            &ValidatedCurrentRunReceipts::from_test_locators([release_run.locator()]),
            false,
        )
        .is_empty()
    );

    let mut covered_by_parent = sample_ignored_baseline("tests/helper.rs", "child");
    covered_by_parent.kind = IgnoreKind::SubprocessHelper;
    covered_by_parent.policy = IgnorePolicy::CoveredByParent;
    assert_eq!(covered_by_parent.validate(), Ok(()));
    let blockers = ignored_test_release_blockers(
        &[covered_by_parent.clone()],
        &ValidatedCurrentRunReceipts::default(),
        false,
    );
    assert_eq!(blockers.len(), 1);
    assert!(blockers[0].contains("remains non-authoritative"));

    covered_by_parent.evidence.receipt = Some(sample_ignore_receipt(Some((
        "tests/parent.rs",
        "parent_case",
    ))));
    assert_eq!(covered_by_parent.validate(), Ok(()));
    let blockers = ignored_test_release_blockers(
        &[covered_by_parent],
        &ValidatedCurrentRunReceipts::default(),
        false,
    );
    assert_eq!(blockers.len(), 1);
    assert!(blockers[0].contains("acyclicity"));
}

#[test]
fn test_regression_guard_release_evaluator_fails_each_independent_gate() {
    let baseline_with = |ignored_tests| RegressionBaseline {
        as_of_phase: "checkpoint_1".to_owned(),
        total_tests: 1,
        passed: 1,
        failed: 0,
        ignored: 0,
        baseline_commit: "deadbeef".to_owned(),
        ignored_tests,
    };
    let green_counts = RegressionCounts {
        total_tests: 1,
        passed: 1,
        failed: 0,
        ignored: 0,
    };
    let empty_inventory = RepositoryIgnoreInventory {
        records: Vec::new(),
        uninspected_sources: Vec::new(),
        soundness_limitations: Vec::new(),
    };
    let empty_baseline = baseline_with(Vec::new());
    let empty_receipts = ValidatedCurrentRunReceipts::default();
    assert!(
        evaluate_release_gate(
            &empty_baseline,
            &green_counts,
            &empty_inventory,
            &empty_receipts,
            false,
        )
        .passes()
    );

    let opaque_inventory = RepositoryIgnoreInventory {
        records: Vec::new(),
        uninspected_sources: vec!["tests/opaque.rs".to_owned()],
        soundness_limitations: Vec::new(),
    };
    let evaluation = evaluate_release_gate(
        &empty_baseline,
        &green_counts,
        &opaque_inventory,
        &empty_receipts,
        false,
    );
    assert!(!evaluation.passes());
    assert!(
        evaluation
            .failure_summary()
            .contains("explicitly uninspected")
    );

    let limited_inventory = RepositoryIgnoreInventory {
        records: Vec::new(),
        uninspected_sources: Vec::new(),
        soundness_limitations: vec!["macro expansion is unresolved".to_owned()],
    };
    let evaluation = evaluate_release_gate(
        &empty_baseline,
        &green_counts,
        &limited_inventory,
        &empty_receipts,
        false,
    );
    assert!(!evaluation.passes());
    assert!(
        evaluation
            .failure_summary()
            .contains("soundness limitation")
    );

    let taxonomy_inventory = RepositoryIgnoreInventory {
        records: vec![IgnoredTestSource {
            source_path: "tests/new.rs".to_owned(),
            test_name: "new_case".to_owned(),
            reason: "unclassified".to_owned(),
            cfg_condition: None,
        }],
        uninspected_sources: Vec::new(),
        soundness_limitations: Vec::new(),
    };
    let evaluation = evaluate_release_gate(
        &empty_baseline,
        &green_counts,
        &taxonomy_inventory,
        &empty_receipts,
        false,
    );
    assert!(!evaluation.passes());
    assert_eq!(evaluation.taxonomy_mismatches.len(), 1);

    let blocker = sample_ignored_baseline("tests/blocker.rs", "case");
    let blocker_inventory = RepositoryIgnoreInventory {
        records: vec![blocker.source_identity()],
        uninspected_sources: Vec::new(),
        soundness_limitations: Vec::new(),
    };
    let evaluation = evaluate_release_gate(
        &baseline_with(vec![blocker]),
        &green_counts,
        &blocker_inventory,
        &empty_receipts,
        false,
    );
    assert!(!evaluation.passes());
    assert_eq!(evaluation.policy_blockers.len(), 1);

    let mut release_run = sample_ignored_baseline("tests/manual.rs", "case");
    release_run.kind = IgnoreKind::Stress;
    release_run.policy = IgnorePolicy::RunForRelease;
    let release_locator = release_run.locator();
    let release_inventory = RepositoryIgnoreInventory {
        records: vec![release_run.source_identity()],
        uninspected_sources: Vec::new(),
        soundness_limitations: Vec::new(),
    };
    assert!(
        !evaluate_release_gate(
            &baseline_with(vec![release_run.clone()]),
            &green_counts,
            &release_inventory,
            &empty_receipts,
            false,
        )
        .passes()
    );
    assert!(
        evaluate_release_gate(
            &baseline_with(vec![release_run]),
            &green_counts,
            &release_inventory,
            &ValidatedCurrentRunReceipts::from_test_locators([release_locator]),
            false,
        )
        .passes()
    );

    let mut covered = sample_ignored_baseline("tests/helper.rs", "child");
    covered.kind = IgnoreKind::SubprocessHelper;
    covered.policy = IgnorePolicy::CoveredByParent;
    let covered_inventory = RepositoryIgnoreInventory {
        records: vec![covered.source_identity()],
        uninspected_sources: Vec::new(),
        soundness_limitations: Vec::new(),
    };
    assert!(
        !evaluate_release_gate(
            &baseline_with(vec![covered.clone()]),
            &green_counts,
            &covered_inventory,
            &empty_receipts,
            false,
        )
        .passes()
    );
    covered.evidence.receipt = Some(sample_ignore_receipt(Some((
        "tests/parent.rs",
        "parent_case",
    ))));
    assert!(
        !evaluate_release_gate(
            &baseline_with(vec![covered]),
            &green_counts,
            &covered_inventory,
            &empty_receipts,
            false,
        )
        .passes()
    );

    let mut live_guard = sample_ignored_baseline(
        "crates/fsqlite-harness/tests/phase5_regression_guard.rs",
        "phase5_regression_guard_full_workspace_against_baseline",
    );
    live_guard.kind = IgnoreKind::ReleaseGate;
    live_guard.policy = IgnorePolicy::RunForRelease;
    let live_inventory = RepositoryIgnoreInventory {
        records: vec![live_guard.source_identity()],
        uninspected_sources: Vec::new(),
        soundness_limitations: Vec::new(),
    };
    assert!(
        !evaluate_release_gate(
            &baseline_with(vec![live_guard.clone()]),
            &green_counts,
            &live_inventory,
            &empty_receipts,
            false,
        )
        .passes()
    );
    assert!(
        evaluate_release_gate(
            &baseline_with(vec![live_guard]),
            &green_counts,
            &live_inventory,
            &empty_receipts,
            true,
        )
        .passes()
    );

    let red_counts = RegressionCounts {
        total_tests: 1,
        passed: 0,
        failed: 1,
        ignored: 0,
    };
    assert!(
        !evaluate_release_gate(
            &empty_baseline,
            &red_counts,
            &empty_inventory,
            &empty_receipts,
            false,
        )
        .passes()
    );
}

#[test]
fn test_regression_guard_baseline_validation_fails_closed() {
    let valid = RegressionBaseline {
        as_of_phase: "checkpoint_1".to_owned(),
        total_tests: 3,
        passed: 3,
        failed: 0,
        ignored: 0,
        baseline_commit: "deadbeef".to_owned(),
        ignored_tests: Vec::new(),
    };
    assert_eq!(valid.validate(), Ok(()));

    let mut inconsistent = valid.clone();
    inconsistent.total_tests = 4;
    assert!(
        inconsistent
            .validate()
            .expect_err("inconsistent totals must fail")
            .contains("does not equal")
    );

    let mut failing = valid.clone();
    failing.total_tests = 4;
    failing.failed = 1;
    assert!(
        failing
            .validate()
            .expect_err("a failing release baseline must fail")
            .contains("zero failures")
    );

    let mut ignored = valid.clone();
    ignored.total_tests = 4;
    ignored.ignored = 1;
    assert_eq!(
        ignored.validate(),
        Ok(()),
        "aggregate ignored counts are telemetry; exact taxonomy owns policy"
    );

    let mut bad_commit = valid;
    bad_commit.baseline_commit = "not-a-commit".to_owned();
    assert!(
        bad_commit
            .validate()
            .expect_err("invalid commit provenance must fail")
            .contains("hexadecimal Git object name")
    );
}

#[test]
fn test_regression_guard_detects_failure() {
    let baseline = RegressionBaseline {
        as_of_phase: "checkpoint_1".to_owned(),
        total_tests: 5_319,
        passed: 5_319,
        failed: 0,
        ignored: 0,
        baseline_commit: "deadbeef".to_owned(),
        ignored_tests: Vec::new(),
    };
    let actual = RegressionCounts {
        total_tests: 5_319,
        passed: 5_317,
        failed: 2,
        ignored: 0,
    };

    let report = compare_against_baseline(&baseline, &actual);
    assert!(
        !report.pass,
        "bead_id={BEAD_ID} case=detect_failure_report_must_fail"
    );
    let reason = report.reason.unwrap_or_default();
    assert!(
        reason.contains("failed increased"),
        "bead_id={BEAD_ID} case=detect_failure_reason reason={reason}"
    );
}

#[test]
fn test_regression_guard_baseline_comparison() {
    let baseline = RegressionBaseline {
        as_of_phase: "checkpoint_1".to_owned(),
        total_tests: 5_319,
        passed: 5_319,
        failed: 0,
        ignored: 0,
        baseline_commit: "deadbeef".to_owned(),
        ignored_tests: Vec::new(),
    };
    let actual = RegressionCounts {
        total_tests: 5_322,
        passed: 5_322,
        failed: 0,
        ignored: 0,
    };

    let report = compare_against_baseline(&baseline, &actual);
    assert!(
        report.pass,
        "bead_id={BEAD_ID} case=baseline_compare_should_pass report={report:?}"
    );
    assert_eq!(
        report.delta.new_tests, 3,
        "bead_id={BEAD_ID} case=baseline_compare_new_tests"
    );
    assert_eq!(
        report.delta.delta_failed, 0,
        "bead_id={BEAD_ID} case=baseline_compare_failed_delta"
    );
}

#[test]
fn test_regression_guard_treats_aggregate_ignored_delta_as_telemetry() {
    let baseline = RegressionBaseline {
        as_of_phase: "checkpoint_1".to_owned(),
        total_tests: 5,
        passed: 5,
        failed: 0,
        ignored: 0,
        baseline_commit: "deadbeef".to_owned(),
        ignored_tests: Vec::new(),
    };
    let actual = RegressionCounts {
        total_tests: 6,
        passed: 5,
        failed: 0,
        ignored: 1,
    };

    let report = compare_against_baseline(&baseline, &actual);
    assert!(
        report.pass,
        "aggregate ignored growth alone must defer to the exact taxonomy: {report:?}"
    );
    assert_eq!(report.delta.delta_ignored, 1);
}

#[test]
#[ignore = "Validates a commit-bound release evidence manifest and canonical workspace transcript against the regression baseline"]
fn phase5_regression_guard_full_workspace_against_baseline() -> Result<(), String> {
    let root = repo_root();
    let captured_head = resolve_current_head(&root)
        .map_err(|error| format!("bead_id={BEAD_ID} case=current_commit error={error}"))?;
    require_pristine_release_checkout(&root)
        .map_err(|error| format!("bead_id={BEAD_ID} case=initial_worktree_check error={error}"))?;
    let baseline_file = baseline_path(&root);
    let baseline = load_regression_baseline(&baseline_file, &root, &captured_head)
        .map_err(|error| format!("bead_id={BEAD_ID} case=load_baseline_failed error={error}"))?;
    let evidence = load_release_evidence_manifest(&root, &captured_head, &baseline)
        .map_err(|error| format!("bead_id={BEAD_ID} case=load_evidence_failed error={error}"))?;
    let live_arguments = std::env::args().collect::<Vec<_>>();
    validate_live_release_guard_invocation(&live_arguments)
        .map_err(|error| format!("bead_id={BEAD_ID} case=live_guard_invocation error={error}"))?;

    eprintln!(
        "{LOG_PREFIX}[phase={}][step=validate_evidence] tested_commit={}",
        baseline.as_of_phase, evidence.tested_commit,
    );

    let counts = evidence.workspace_counts;

    eprintln!(
        "{LOG_PREFIX}[phase={}][step=parse_results] total={} passed={} failed={} ignored={}",
        baseline.as_of_phase, counts.total_tests, counts.passed, counts.failed, counts.ignored
    );

    let inventory = collect_repository_ignored_tests(&root, UNINSPECTED_RUST_SOURCE_PATHS)
        .map_err(|error| format!("bead_id={BEAD_ID} case=source_inventory_failed error={error}"))?;
    let evaluation = evaluate_release_gate(
        &baseline,
        &counts,
        &inventory,
        &evidence.current_run_receipts,
        true,
    );
    eprintln!(
        "{LOG_PREFIX}[phase={}][step=compare_baseline] delta_passed={} delta_failed={} delta_ignored={} new_tests={}",
        baseline.as_of_phase,
        evaluation.aggregate.delta.delta_passed,
        evaluation.aggregate.delta.delta_failed,
        evaluation.aggregate.delta.delta_ignored,
        evaluation.aggregate.delta.new_tests
    );

    if evaluation.passes() {
        let final_head = resolve_current_head(&root)
            .map_err(|error| format!("bead_id={BEAD_ID} case=final_commit error={error}"))?;
        if final_head != captured_head {
            return Err(format!(
                "bead_id={BEAD_ID} case=repository_moved error=HEAD changed during release-gate evaluation"
            ));
        }
        require_pristine_release_checkout(&root).map_err(|error| {
            format!("bead_id={BEAD_ID} case=final_worktree_check error={error}")
        })?;
        eprintln!(
            "{LOG_PREFIX}[phase={}][result=PASS] aggregate counts, evidence manifest, ignored-test taxonomy, and release policies validated at commit {}",
            baseline.as_of_phase, captured_head
        );
        return Ok(());
    }

    for failed in extract_failed_tests(&evidence.workspace_transcript) {
        eprintln!(
            "{LOG_PREFIX}[phase={}][step=failures] test_name=\"{}\"",
            baseline.as_of_phase, failed
        );
    }

    let reason = evaluation.failure_summary();
    Err(format!(
        "{LOG_PREFIX}[phase={}][result=FAIL] {reason}; baseline_commit={} tested_commit={}",
        baseline.as_of_phase, baseline.baseline_commit, evidence.tested_commit
    ))
}
