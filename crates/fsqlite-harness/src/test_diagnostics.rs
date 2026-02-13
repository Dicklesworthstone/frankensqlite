//! Unit failure diagnostics contract and shared helpers (bd-mblr.6.6).
//!
//! Standardizes rich failure output for unit tests across all crates:
//! - **Invariant ID**: Every assertion links to a spec invariant or bead.
//! - **Seed/fixture ID**: Reproducibility data is always surfaced on failure.
//! - **State snapshot**: Compact representation of relevant state at failure point.
//! - **Diff hints**: When comparing structured data, the diff is highlighted.
//!
//! # Diagnostics Contract
//!
//! Every test assertion message in this project MUST include:
//!
//! 1. `bead_id=<ID>` — the bead that owns this test.
//! 2. `case=<name>` — a short, unique identifier for this assertion within the test.
//! 3. At least one of:
//!    - `seed=<hex>` — the deterministic seed (for reproducibility).
//!    - `fixture=<id>` — the fixture ID (for test data identification).
//!    - `invariant=<spec-ref>` — the spec invariant being checked.
//!
//! Optional (but recommended for complex failures):
//! - `expected=<val>` / `actual=<val>` — what was expected vs. what happened.
//! - `state=<compact-repr>` — snapshot of relevant state.
//! - `hint=<human-readable>` — actionable debugging guidance.
//!
//! # Example
//!
//! ```rust
//! use fsqlite_harness::test_diagnostics::{DiagContext, diag_assert_eq};
//!
//! let ctx = DiagContext::new("bd-mblr.6.6")
//!     .case("seed_roundtrip")
//!     .seed(0xDEAD_BEEF)
//!     .fixture("page-empty-leaf-table");
//!
//! // Rich assertion with full context on failure:
//! diag_assert_eq!(ctx, 42, 42);
//! ```

use std::fmt;
use std::fmt::Write as _;

/// Bead identifier for this module's own tests.
const BEAD_ID: &str = "bd-mblr.6.6";

// ─── Diagnostics Context ────────────────────────────────────────────────

/// Structured diagnostics context attached to every assertion.
///
/// Collects bead ID, case name, seed, fixture ID, invariant reference,
/// and optional state/hints for rich failure messages.
#[derive(Debug, Clone)]
pub struct DiagContext {
    /// The bead that owns this test (required).
    pub bead_id: String,
    /// Short identifier for this assertion case.
    pub case: Option<String>,
    /// Deterministic seed for reproducibility.
    pub seed: Option<u64>,
    /// Fixture ID being tested.
    pub fixture: Option<String>,
    /// Spec invariant reference.
    pub invariant: Option<String>,
    /// Compact state snapshot.
    pub state: Option<String>,
    /// Human-readable debugging hint.
    pub hint: Option<String>,
}

impl DiagContext {
    /// Create a new diagnostics context with the given bead ID.
    #[must_use]
    pub fn new(bead_id: &str) -> Self {
        Self {
            bead_id: bead_id.to_owned(),
            case: None,
            seed: None,
            fixture: None,
            invariant: None,
            state: None,
            hint: None,
        }
    }

    /// Set the assertion case name (fluent builder).
    #[must_use]
    pub fn case(mut self, case: &str) -> Self {
        self.case = Some(case.to_owned());
        self
    }

    /// Set the deterministic seed (fluent builder).
    #[must_use]
    pub const fn seed(mut self, seed: u64) -> Self {
        self.seed = Some(seed);
        self
    }

    /// Set the fixture ID (fluent builder).
    #[must_use]
    pub fn fixture(mut self, fixture: &str) -> Self {
        self.fixture = Some(fixture.to_owned());
        self
    }

    /// Set the spec invariant reference (fluent builder).
    #[must_use]
    pub fn invariant(mut self, inv: &str) -> Self {
        self.invariant = Some(inv.to_owned());
        self
    }

    /// Attach a compact state snapshot (fluent builder).
    #[must_use]
    pub fn state(mut self, state: &str) -> Self {
        self.state = Some(state.to_owned());
        self
    }

    /// Attach a debugging hint (fluent builder).
    #[must_use]
    pub fn hint(mut self, hint: &str) -> Self {
        self.hint = Some(hint.to_owned());
        self
    }

    /// Format as a key-value diagnostic string for assertion messages.
    ///
    /// Output format:
    /// ```text
    /// bead_id=bd-xxx case=foo seed=0xDEADBEEF fixture=page-1 invariant=INV-1
    /// ```
    #[must_use]
    pub fn format_kv(&self) -> String {
        let mut parts = Vec::with_capacity(7);
        parts.push(format!("bead_id={}", self.bead_id));

        if let Some(ref c) = self.case {
            parts.push(format!("case={c}"));
        }
        if let Some(s) = self.seed {
            parts.push(format!("seed=0x{s:016X}"));
        }
        if let Some(ref f) = self.fixture {
            parts.push(format!("fixture={f}"));
        }
        if let Some(ref inv) = self.invariant {
            parts.push(format!("invariant={inv}"));
        }
        if let Some(ref st) = self.state {
            parts.push(format!("state={st}"));
        }
        if let Some(ref h) = self.hint {
            parts.push(format!("hint={h}"));
        }

        parts.join(" ")
    }
}

impl fmt::Display for DiagContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.format_kv())
    }
}

// ─── Diff Helpers ───────────────────────────────────────────────────────

/// Compute a simple line-based diff between two strings.
///
/// Returns a compact diff string showing lines that differ, with `−` for
/// missing and `+` for extra lines. Returns `None` if the strings are equal.
#[must_use]
pub fn simple_diff(expected: &str, actual: &str) -> Option<String> {
    if expected == actual {
        return None;
    }

    let exp_lines: Vec<&str> = expected.lines().collect();
    let act_lines: Vec<&str> = actual.lines().collect();

    let mut diff = String::new();
    let max_len = exp_lines.len().max(act_lines.len());

    for i in 0..max_len {
        let exp = exp_lines.get(i).copied().unwrap_or("");
        let act = act_lines.get(i).copied().unwrap_or("");
        if exp != act {
            if !exp.is_empty() {
                let _ = write!(diff, "  line {}: - {exp}\n", i + 1);
            }
            if !act.is_empty() {
                let _ = write!(diff, "  line {}: + {act}\n", i + 1);
            }
        }
    }

    Some(diff)
}

/// Format a compact state snapshot for a collection of key-value pairs.
///
/// Produces: `{key1=val1, key2=val2, ...}`
#[must_use]
pub fn snapshot_kv(pairs: &[(&str, &str)]) -> String {
    let inner: Vec<String> = pairs.iter().map(|(k, v)| format!("{k}={v}")).collect();
    format!("{{{}}}", inner.join(", "))
}

/// Format a hex dump of the first N bytes of a byte slice.
///
/// Useful for showing page header state in failure messages.
#[must_use]
pub fn hex_preview(data: &[u8], max_bytes: usize) -> String {
    let take = data.len().min(max_bytes);
    let hex_str: Vec<String> = data[..take].iter().map(|b| format!("{b:02X}")).collect();
    let suffix = if data.len() > max_bytes {
        format!("..({} more)", data.len() - max_bytes)
    } else {
        String::new()
    };
    format!("[{}]{suffix}", hex_str.join(" "))
}

/// Build a repro command string for re-running a specific failing test.
///
/// Example output: `cargo test -p fsqlite-harness --lib unit_fixtures::tests::seed_roundtrip`
#[must_use]
pub fn repro_command(crate_name: &str, test_path: &str) -> String {
    format!("cargo test -p {crate_name} -- {test_path} --exact --nocapture")
}

// ─── Assertion Macros ───────────────────────────────────────────────────

/// Assert equality with rich diagnostics context.
///
/// On failure, prints the `DiagContext` key-value string plus expected/actual
/// values. Usage:
///
/// ```rust
/// use fsqlite_harness::test_diagnostics::{DiagContext, diag_assert_eq};
///
/// let ctx = DiagContext::new("bd-test").case("basic");
/// diag_assert_eq!(ctx, 1 + 1, 2);
/// ```
#[macro_export]
macro_rules! diag_assert_eq {
    ($ctx:expr, $left:expr, $right:expr) => {
        let __diag_left = &$left;
        let __diag_right = &$right;
        if __diag_left != __diag_right {
            panic!(
                "{} expected={:?} actual={:?}",
                $ctx.format_kv(),
                __diag_right,
                __diag_left,
            );
        }
    };
    ($ctx:expr, $left:expr, $right:expr, $($arg:tt)+) => {
        let __diag_left = &$left;
        let __diag_right = &$right;
        if __diag_left != __diag_right {
            panic!(
                "{} expected={:?} actual={:?} {}",
                $ctx.format_kv(),
                __diag_right,
                __diag_left,
                format_args!($($arg)+),
            );
        }
    };
}

/// Assert inequality with rich diagnostics context.
#[macro_export]
macro_rules! diag_assert_ne {
    ($ctx:expr, $left:expr, $right:expr) => {
        let __diag_left = &$left;
        let __diag_right = &$right;
        if __diag_left == __diag_right {
            panic!(
                "{} values_should_differ={:?}",
                $ctx.format_kv(),
                __diag_left,
            );
        }
    };
    ($ctx:expr, $left:expr, $right:expr, $($arg:tt)+) => {
        let __diag_left = &$left;
        let __diag_right = &$right;
        if __diag_left == __diag_right {
            panic!(
                "{} values_should_differ={:?} {}",
                $ctx.format_kv(),
                __diag_left,
                format_args!($($arg)+),
            );
        }
    };
}

/// Assert a condition with rich diagnostics context.
#[macro_export]
macro_rules! diag_assert {
    ($ctx:expr, $cond:expr) => {
        if !$cond {
            panic!(
                "{} assertion_failed={}",
                $ctx.format_kv(),
                stringify!($cond),
            );
        }
    };
    ($ctx:expr, $cond:expr, $($arg:tt)+) => {
        if !$cond {
            panic!(
                "{} assertion_failed={} {}",
                $ctx.format_kv(),
                stringify!($cond),
                format_args!($($arg)+),
            );
        }
    };
}

// ─── Diagnostic Report ──────────────────────────────────────────────────

/// A structured failure report for richer-than-panic diagnostics.
///
/// Collects multiple diagnostic findings during a test run, then produces
/// a unified summary. Useful for tests that check many invariants and want
/// to report all failures (not just the first).
#[derive(Debug, Clone, Default)]
pub struct DiagReport {
    findings: Vec<DiagFinding>,
}

/// A single diagnostic finding (potential failure).
#[derive(Debug, Clone)]
pub struct DiagFinding {
    /// The diagnostics context for this finding.
    pub context: DiagContext,
    /// Severity level.
    pub severity: Severity,
    /// Human-readable description of what went wrong.
    pub message: String,
    /// Expected value (if applicable).
    pub expected: Option<String>,
    /// Actual value (if applicable).
    pub actual: Option<String>,
}

/// Finding severity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    /// Hard failure — test should abort.
    Error,
    /// Soft failure — test continues but reports degradation.
    Warning,
}

impl DiagReport {
    /// Create a new empty report.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a finding to the report.
    pub fn add(&mut self, finding: DiagFinding) {
        self.findings.push(finding);
    }

    /// Record an error finding with context.
    pub fn error(&mut self, ctx: &DiagContext, message: &str) {
        self.findings.push(DiagFinding {
            context: ctx.clone(),
            severity: Severity::Error,
            message: message.to_owned(),
            expected: None,
            actual: None,
        });
    }

    /// Record an error finding with expected/actual values.
    pub fn error_mismatch(
        &mut self,
        ctx: &DiagContext,
        message: &str,
        expected: &str,
        actual: &str,
    ) {
        self.findings.push(DiagFinding {
            context: ctx.clone(),
            severity: Severity::Error,
            message: message.to_owned(),
            expected: Some(expected.to_owned()),
            actual: Some(actual.to_owned()),
        });
    }

    /// Record a warning finding.
    pub fn warn(&mut self, ctx: &DiagContext, message: &str) {
        self.findings.push(DiagFinding {
            context: ctx.clone(),
            severity: Severity::Warning,
            message: message.to_owned(),
            expected: None,
            actual: None,
        });
    }

    /// Number of error-severity findings.
    #[must_use]
    pub fn error_count(&self) -> usize {
        self.findings
            .iter()
            .filter(|f| f.severity == Severity::Error)
            .count()
    }

    /// Number of warning-severity findings.
    #[must_use]
    pub fn warning_count(&self) -> usize {
        self.findings
            .iter()
            .filter(|f| f.severity == Severity::Warning)
            .count()
    }

    /// True if there are no error findings.
    #[must_use]
    pub fn is_ok(&self) -> bool {
        self.error_count() == 0
    }

    /// All findings.
    #[must_use]
    pub fn findings(&self) -> &[DiagFinding] {
        &self.findings
    }

    /// Render the report as a human-readable string.
    #[must_use]
    pub fn render(&self) -> String {
        if self.findings.is_empty() {
            return "DiagReport: 0 findings (all clear)".to_owned();
        }

        let mut out = String::new();
        let _ = write!(
            out,
            "DiagReport: {} error(s), {} warning(s)\n",
            self.error_count(),
            self.warning_count()
        );

        for (i, f) in self.findings.iter().enumerate() {
            let sev = match f.severity {
                Severity::Error => "ERROR",
                Severity::Warning => "WARN",
            };
            let _ = write!(
                out,
                "  [{}] #{}: {}\n",
                sev,
                i + 1,
                f.context.format_kv()
            );
            let _ = write!(out, "        {}\n", f.message);
            if let Some(ref e) = f.expected {
                let _ = write!(out, "        expected: {e}\n");
            }
            if let Some(ref a) = f.actual {
                let _ = write!(out, "        actual:   {a}\n");
            }
        }

        out
    }

    /// Panic with the full report if there are any errors.
    ///
    /// Call this at the end of a multi-assertion test to get a unified
    /// failure summary instead of dying on the first assertion.
    pub fn assert_ok(&self) {
        if !self.is_ok() {
            panic!("{}", self.render());
        }
    }
}

impl fmt::Display for DiagReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.render())
    }
}

// ─── Adoption Checklist ─────────────────────────────────────────────────

/// Items in the diagnostics adoption checklist.
///
/// Each item describes a pattern that should be present in test code.
#[derive(Debug, Clone)]
pub struct AdoptionChecklistItem {
    /// Short identifier for the checklist item.
    pub id: String,
    /// What the test code should do.
    pub requirement: String,
    /// Example of compliant code.
    pub example: String,
}

/// Build the diagnostics adoption checklist.
///
/// This is the reference document for migrating existing tests to the
/// standardized diagnostics contract.
#[must_use]
pub fn build_adoption_checklist() -> Vec<AdoptionChecklistItem> {
    vec![
        AdoptionChecklistItem {
            id: "D-1".to_owned(),
            requirement: "Every test module defines const BEAD_ID: &str".to_owned(),
            example: r#"const BEAD_ID: &str = "bd-xxx";"#.to_owned(),
        },
        AdoptionChecklistItem {
            id: "D-2".to_owned(),
            requirement: "Every assertion message starts with bead_id= and case=".to_owned(),
            example: r#"assert_eq!(a, b, "bead_id={BEAD_ID} case=roundtrip");"#.to_owned(),
        },
        AdoptionChecklistItem {
            id: "D-3".to_owned(),
            requirement: "Tests using deterministic seeds include seed= in failure messages"
                .to_owned(),
            example: r#"assert_eq!(a, b, "bead_id={BEAD_ID} case=hash seed=0x{seed:016X}");"#
                .to_owned(),
        },
        AdoptionChecklistItem {
            id: "D-4".to_owned(),
            requirement: "Tests using fixtures include fixture= in failure messages".to_owned(),
            example: r#"assert_eq!(a, b, "bead_id={BEAD_ID} case=validate fixture={}", f.id);"#
                .to_owned(),
        },
        AdoptionChecklistItem {
            id: "D-5".to_owned(),
            requirement: "Complex tests use DiagContext for structured assertion messages"
                .to_owned(),
            example: r#"let ctx = DiagContext::new(BEAD_ID).case("complex").seed(seed);
diag_assert_eq!(ctx, actual, expected);"#
                .to_owned(),
        },
        AdoptionChecklistItem {
            id: "D-6".to_owned(),
            requirement: "Multi-invariant tests use DiagReport for unified failure summaries"
                .to_owned(),
            example: r#"let mut report = DiagReport::new();
// ... check multiple invariants ...
report.assert_ok();"#
                .to_owned(),
        },
        AdoptionChecklistItem {
            id: "D-7".to_owned(),
            requirement: "Page/binary data failures include hex_preview of relevant bytes"
                .to_owned(),
            example: r#"let preview = hex_preview(&page_data, 16);
assert_eq!(flag, 0x0D, "bead_id={BEAD_ID} case=page_type header={preview}");"#
                .to_owned(),
        },
        AdoptionChecklistItem {
            id: "D-8".to_owned(),
            requirement: "String comparison failures include simple_diff output".to_owned(),
            example: r#"if let Some(diff) = simple_diff(&expected, &actual) {
    panic!("bead_id={BEAD_ID} case=output_match\n{diff}");
}"#
            .to_owned(),
        },
    ]
}

// ─── Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── DiagContext tests ────────────────────────────────────────────

    #[test]
    fn diag_context_format_kv_minimal() {
        let ctx = DiagContext::new("bd-test");
        let kv = ctx.format_kv();
        assert!(
            kv.starts_with("bead_id=bd-test"),
            "bead_id={BEAD_ID} case=ctx_kv_minimal: got {kv}"
        );
    }

    #[test]
    fn diag_context_format_kv_full() {
        let ctx = DiagContext::new("bd-test")
            .case("full")
            .seed(0xCAFE)
            .fixture("page-1")
            .invariant("INV-1")
            .state("{pg=1}")
            .hint("check header");
        let kv = ctx.format_kv();
        assert!(
            kv.contains("bead_id=bd-test"),
            "bead_id={BEAD_ID} case=ctx_kv_bead"
        );
        assert!(
            kv.contains("case=full"),
            "bead_id={BEAD_ID} case=ctx_kv_case"
        );
        assert!(
            kv.contains("seed=0x000000000000CAFE"),
            "bead_id={BEAD_ID} case=ctx_kv_seed: got {kv}"
        );
        assert!(
            kv.contains("fixture=page-1"),
            "bead_id={BEAD_ID} case=ctx_kv_fixture"
        );
        assert!(
            kv.contains("invariant=INV-1"),
            "bead_id={BEAD_ID} case=ctx_kv_invariant"
        );
        assert!(
            kv.contains("state={pg=1}"),
            "bead_id={BEAD_ID} case=ctx_kv_state"
        );
        assert!(
            kv.contains("hint=check header"),
            "bead_id={BEAD_ID} case=ctx_kv_hint"
        );
    }

    #[test]
    fn diag_context_display_matches_format_kv() {
        let ctx = DiagContext::new("bd-test").case("display");
        assert_eq!(
            format!("{ctx}"),
            ctx.format_kv(),
            "bead_id={BEAD_ID} case=ctx_display_eq"
        );
    }

    // ── Diff helpers tests ──────────────────────────────────────────

    #[test]
    fn simple_diff_equal_returns_none() {
        assert!(
            simple_diff("hello", "hello").is_none(),
            "bead_id={BEAD_ID} case=diff_equal"
        );
    }

    #[test]
    fn simple_diff_different_shows_lines() {
        let diff = simple_diff("line1\nline2", "line1\nline3");
        assert!(diff.is_some(), "bead_id={BEAD_ID} case=diff_different_some");
        let d = diff.unwrap();
        assert!(
            d.contains("line2"),
            "bead_id={BEAD_ID} case=diff_contains_old: {d}"
        );
        assert!(
            d.contains("line3"),
            "bead_id={BEAD_ID} case=diff_contains_new: {d}"
        );
    }

    #[test]
    fn simple_diff_empty_vs_content() {
        let diff = simple_diff("", "hello");
        assert!(
            diff.is_some(),
            "bead_id={BEAD_ID} case=diff_empty_vs_content"
        );
    }

    #[test]
    fn snapshot_kv_formatting() {
        let snap = snapshot_kv(&[("pg", "1"), ("cells", "5"), ("type", "leaf")]);
        assert_eq!(
            snap, "{pg=1, cells=5, type=leaf}",
            "bead_id={BEAD_ID} case=snapshot_kv_format"
        );
    }

    #[test]
    fn snapshot_kv_empty() {
        let snap = snapshot_kv(&[]);
        assert_eq!(snap, "{}", "bead_id={BEAD_ID} case=snapshot_kv_empty");
    }

    #[test]
    fn hex_preview_short() {
        let data = [0xCA, 0xFE, 0xBA, 0xBE];
        let preview = hex_preview(&data, 8);
        assert_eq!(
            preview, "[CA FE BA BE]",
            "bead_id={BEAD_ID} case=hex_preview_short"
        );
    }

    #[test]
    fn hex_preview_truncated() {
        let data = [0x01, 0x02, 0x03, 0x04, 0x05];
        let preview = hex_preview(&data, 3);
        assert_eq!(
            preview, "[01 02 03]..(2 more)",
            "bead_id={BEAD_ID} case=hex_preview_truncated"
        );
    }

    #[test]
    fn hex_preview_empty() {
        let preview = hex_preview(&[], 8);
        assert_eq!(preview, "[]", "bead_id={BEAD_ID} case=hex_preview_empty");
    }

    #[test]
    fn repro_command_format() {
        let cmd = repro_command("fsqlite-harness", "unit_fixtures::tests::seed_roundtrip");
        assert!(
            cmd.contains("cargo test"),
            "bead_id={BEAD_ID} case=repro_cmd_cargo"
        );
        assert!(
            cmd.contains("-p fsqlite-harness"),
            "bead_id={BEAD_ID} case=repro_cmd_crate"
        );
        assert!(
            cmd.contains("--exact"),
            "bead_id={BEAD_ID} case=repro_cmd_exact"
        );
    }

    // ── Assertion macro tests ───────────────────────────────────────

    #[test]
    fn diag_assert_eq_passes_on_equal() {
        let ctx = DiagContext::new(BEAD_ID).case("macro_eq_pass");
        diag_assert_eq!(ctx, 42, 42);
    }

    #[test]
    #[should_panic(expected = "bead_id=bd-mblr.6.6")]
    fn diag_assert_eq_panics_with_context() {
        let ctx = DiagContext::new(BEAD_ID).case("macro_eq_fail");
        diag_assert_eq!(ctx, 1, 2);
    }

    #[test]
    fn diag_assert_ne_passes_on_different() {
        let ctx = DiagContext::new(BEAD_ID).case("macro_ne_pass");
        diag_assert_ne!(ctx, 1, 2);
    }

    #[test]
    #[should_panic(expected = "bead_id=bd-mblr.6.6")]
    fn diag_assert_ne_panics_with_context() {
        let ctx = DiagContext::new(BEAD_ID).case("macro_ne_fail");
        diag_assert_ne!(ctx, 42, 42);
    }

    #[test]
    fn diag_assert_passes_on_true() {
        let ctx = DiagContext::new(BEAD_ID).case("macro_assert_pass");
        diag_assert!(ctx, 1 + 1 == 2);
    }

    #[test]
    #[should_panic(expected = "bead_id=bd-mblr.6.6")]
    fn diag_assert_panics_with_context() {
        let ctx = DiagContext::new(BEAD_ID).case("macro_assert_fail");
        diag_assert!(ctx, false);
    }

    #[test]
    fn diag_assert_eq_with_extra_message() {
        let ctx = DiagContext::new(BEAD_ID).case("macro_eq_extra");
        diag_assert_eq!(ctx, 42, 42, "extra info {}", "here");
    }

    // ── DiagReport tests ────────────────────────────────────────────

    #[test]
    fn diag_report_empty_is_ok() {
        let report = DiagReport::new();
        assert!(report.is_ok(), "bead_id={BEAD_ID} case=report_empty_ok");
        assert_eq!(
            report.error_count(),
            0,
            "bead_id={BEAD_ID} case=report_empty_errors"
        );
    }

    #[test]
    fn diag_report_with_errors() {
        let mut report = DiagReport::new();
        let ctx = DiagContext::new(BEAD_ID).case("report_err");
        report.error(&ctx, "something went wrong");
        assert!(
            !report.is_ok(),
            "bead_id={BEAD_ID} case=report_with_error_not_ok"
        );
        assert_eq!(
            report.error_count(),
            1,
            "bead_id={BEAD_ID} case=report_error_count"
        );
    }

    #[test]
    fn diag_report_with_warnings_is_ok() {
        let mut report = DiagReport::new();
        let ctx = DiagContext::new(BEAD_ID).case("report_warn");
        report.warn(&ctx, "minor issue");
        assert!(report.is_ok(), "bead_id={BEAD_ID} case=report_warn_is_ok");
        assert_eq!(
            report.warning_count(),
            1,
            "bead_id={BEAD_ID} case=report_warn_count"
        );
    }

    #[test]
    fn diag_report_mismatch_error() {
        let mut report = DiagReport::new();
        let ctx = DiagContext::new(BEAD_ID).case("report_mismatch");
        report.error_mismatch(&ctx, "value differs", "42", "43");
        let rendered = report.render();
        assert!(
            rendered.contains("expected: 42"),
            "bead_id={BEAD_ID} case=report_mismatch_expected: {rendered}"
        );
        assert!(
            rendered.contains("actual:   43"),
            "bead_id={BEAD_ID} case=report_mismatch_actual: {rendered}"
        );
    }

    #[test]
    fn diag_report_render_empty() {
        let report = DiagReport::new();
        let rendered = report.render();
        assert!(
            rendered.contains("0 findings"),
            "bead_id={BEAD_ID} case=report_render_empty: {rendered}"
        );
    }

    #[test]
    fn diag_report_render_multi() {
        let mut report = DiagReport::new();
        let ctx = DiagContext::new(BEAD_ID).case("multi");
        report.error(&ctx, "first error");
        report.warn(&ctx, "a warning");
        report.error(&ctx, "second error");
        let rendered = report.render();
        assert!(
            rendered.contains("2 error(s)"),
            "bead_id={BEAD_ID} case=report_multi_errors: {rendered}"
        );
        assert!(
            rendered.contains("1 warning(s)"),
            "bead_id={BEAD_ID} case=report_multi_warnings: {rendered}"
        );
    }

    #[test]
    #[should_panic(expected = "DiagReport")]
    fn diag_report_assert_ok_panics_on_errors() {
        let mut report = DiagReport::new();
        let ctx = DiagContext::new(BEAD_ID).case("assert_ok_fail");
        report.error(&ctx, "should panic");
        report.assert_ok();
    }

    #[test]
    fn diag_report_assert_ok_passes_when_clean() {
        let report = DiagReport::new();
        report.assert_ok(); // Should not panic.
    }

    // ── Adoption checklist tests ────────────────────────────────────

    #[test]
    fn adoption_checklist_has_items() {
        let checklist = build_adoption_checklist();
        assert!(
            checklist.len() >= 8,
            "bead_id={BEAD_ID} case=checklist_count: got {}",
            checklist.len()
        );
    }

    #[test]
    fn adoption_checklist_ids_are_unique() {
        let checklist = build_adoption_checklist();
        let mut seen = std::collections::HashSet::new();
        for item in &checklist {
            assert!(
                seen.insert(&item.id),
                "bead_id={BEAD_ID} case=checklist_unique_ids: duplicate {}",
                item.id
            );
        }
    }

    #[test]
    fn adoption_checklist_items_have_examples() {
        for item in &build_adoption_checklist() {
            assert!(
                !item.example.is_empty(),
                "bead_id={BEAD_ID} case=checklist_has_example: {} missing example",
                item.id
            );
        }
    }
}
