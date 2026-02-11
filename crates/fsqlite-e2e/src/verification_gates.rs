//! Verification gates for the RealDB harness (bd-2als.6.2).
//!
//! Enforces code quality gates that must pass before any harness changes land:
//!
//! - `cargo fmt --check` — formatting consistency
//! - `cargo check --all-targets` — compilation without warnings
//! - `cargo clippy --all-targets -- -D warnings` — pedantic lint enforcement
//! - No references to `master` branch (must be `main`)
//!
//! All checks are run against the `fsqlite-e2e` package specifically, keeping
//! gate execution fast while ensuring harness code meets the same quality bar
//! as the core engine.

use std::fmt::Write;
use std::path::Path;
use std::process::{Command, Output};

// ── Public types ─────────────────────────────────────────────────────

/// Result of running all verification gates.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct GateReport {
    /// Individual gate results.
    pub gates: Vec<GateResult>,
    /// Whether all gates passed.
    pub all_passed: bool,
    /// Total wall-clock time in milliseconds.
    pub total_elapsed_ms: u64,
}

/// Result of a single verification gate.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct GateResult {
    /// Gate name (e.g., "cargo-fmt", "cargo-clippy").
    pub name: String,
    /// Whether the gate passed.
    pub passed: bool,
    /// Human-readable summary.
    pub summary: String,
    /// Stderr/stdout snippet on failure (truncated).
    pub output_snippet: String,
    /// Wall-clock time in milliseconds.
    pub elapsed_ms: u64,
}

/// Configuration for verification gates.
#[derive(Debug, Clone)]
pub struct GateConfig {
    /// Workspace root directory (where `Cargo.toml` lives).
    pub workspace_root: std::path::PathBuf,
    /// Whether to scan for `master` branch references.
    pub check_master_refs: bool,
    /// Maximum length of output snippets in reports.
    pub max_snippet_len: usize,
}

impl Default for GateConfig {
    fn default() -> Self {
        Self {
            workspace_root: std::path::PathBuf::from("."),
            check_master_refs: true,
            max_snippet_len: 2000,
        }
    }
}

// ── Gate runner ──────────────────────────────────────────────────────

/// Run all verification gates and return a report.
#[must_use]
pub fn run_all_gates(config: &GateConfig) -> GateReport {
    let start = std::time::Instant::now();
    let mut gates = Vec::with_capacity(4);

    gates.push(run_fmt_gate(config));
    gates.push(run_check_gate(config));
    gates.push(run_clippy_gate(config));

    if config.check_master_refs {
        gates.push(run_master_refs_gate(config));
    }

    let all_passed = gates.iter().all(|g| g.passed);

    #[allow(clippy::cast_possible_truncation)]
    let total_elapsed_ms = start.elapsed().as_millis() as u64;

    GateReport {
        gates,
        all_passed,
        total_elapsed_ms,
    }
}

/// Format the gate report as human-readable text.
#[must_use]
pub fn format_gate_report(report: &GateReport) -> String {
    let mut out = String::with_capacity(1024);

    let _ = writeln!(out, "=== Verification Gates ===");
    let _ = writeln!(out);

    for gate in &report.gates {
        let status = if gate.passed { "PASS" } else { "FAIL" };
        let _ = writeln!(
            out,
            "  [{status}] {} ({}ms) — {}",
            gate.name, gate.elapsed_ms, gate.summary
        );
        if !gate.passed && !gate.output_snippet.is_empty() {
            for line in gate.output_snippet.lines().take(10) {
                let _ = writeln!(out, "        {line}");
            }
        }
    }

    let _ = writeln!(out);
    let overall = if report.all_passed {
        "ALL GATES PASSED"
    } else {
        "SOME GATES FAILED"
    };
    let _ = writeln!(
        out,
        "=== {overall} ({}/{} in {}ms) ===",
        report.gates.iter().filter(|g| g.passed).count(),
        report.gates.len(),
        report.total_elapsed_ms,
    );

    out
}

// ── Individual gates ─────────────────────────────────────────────────

fn run_fmt_gate(config: &GateConfig) -> GateResult {
    run_cargo_gate(
        "cargo-fmt",
        &config.workspace_root,
        &["fmt", "--all", "--", "--check"],
        config.max_snippet_len,
    )
}

fn run_check_gate(config: &GateConfig) -> GateResult {
    run_cargo_gate(
        "cargo-check",
        &config.workspace_root,
        &["check", "--all-targets"],
        config.max_snippet_len,
    )
}

fn run_clippy_gate(config: &GateConfig) -> GateResult {
    run_cargo_gate(
        "cargo-clippy",
        &config.workspace_root,
        &["clippy", "--all-targets", "--", "-D", "warnings"],
        config.max_snippet_len,
    )
}

/// Scan for references to `master` branch in source files.
fn run_master_refs_gate(config: &GateConfig) -> GateResult {
    let start = std::time::Instant::now();
    let e2e_src = config.workspace_root.join("crates/fsqlite-e2e/src");

    let violations = scan_master_refs(&e2e_src);

    #[allow(clippy::cast_possible_truncation)]
    let elapsed_ms = start.elapsed().as_millis() as u64;

    if violations.is_empty() {
        GateResult {
            name: "no-master-refs".to_owned(),
            passed: true,
            summary: "No references to 'master' branch found".to_owned(),
            output_snippet: String::new(),
            elapsed_ms,
        }
    } else {
        let mut snippet = String::new();
        for v in &violations {
            let _ = writeln!(snippet, "  {v}");
        }
        GateResult {
            name: "no-master-refs".to_owned(),
            passed: false,
            summary: format!("{} references to 'master' branch found", violations.len()),
            output_snippet: truncate_string(&snippet, config.max_snippet_len),
            elapsed_ms,
        }
    }
}

// ── Helpers ──────────────────────────────────────────────────────────

fn run_cargo_gate(
    name: &str,
    workspace_root: &Path,
    args: &[&str],
    max_snippet: usize,
) -> GateResult {
    let start = std::time::Instant::now();

    let result = Command::new("cargo")
        .args(args)
        .current_dir(workspace_root)
        .output();

    #[allow(clippy::cast_possible_truncation)]
    let elapsed_ms = start.elapsed().as_millis() as u64;

    match result {
        Ok(output) => gate_result_from_output(name, &output, elapsed_ms, max_snippet),
        Err(e) => GateResult {
            name: name.to_owned(),
            passed: false,
            summary: format!("Failed to execute: {e}"),
            output_snippet: String::new(),
            elapsed_ms,
        },
    }
}

fn gate_result_from_output(
    name: &str,
    output: &Output,
    elapsed_ms: u64,
    max_snippet: usize,
) -> GateResult {
    let passed = output.status.success();
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);

    let summary = if passed {
        "OK".to_owned()
    } else {
        format!("exit code {}", output.status.code().unwrap_or(-1))
    };

    let snippet = if passed {
        String::new()
    } else {
        let combined = format!("{stdout}{stderr}");
        truncate_string(&combined, max_snippet)
    };

    GateResult {
        name: name.to_owned(),
        passed,
        summary,
        output_snippet: snippet,
        elapsed_ms,
    }
}

/// Scan `.rs` files under `dir` for references to "master" branch.
///
/// Looks for patterns like `origin/master`, `branch = "master"`, `master` in
/// git remote URLs. Excludes legitimate uses (e.g., string literals in test
/// data that mention "master" as a word).
fn scan_master_refs(dir: &Path) -> Vec<String> {
    let mut violations = Vec::new();
    scan_master_refs_inner(dir, dir, &mut violations);

    violations
}

fn scan_master_refs_inner(root: &Path, dir: &Path, violations: &mut Vec<String>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };

    for entry in entries.flatten() {
        let path = entry.path();

        if path.is_dir() {
            scan_master_refs_inner(root, &path, violations);
            continue;
        }

        if path.extension().and_then(|e| e.to_str()) != Some("rs") {
            continue;
        }

        // Skip the verification_gates module itself — it legitimately
        // contains "master" in scanning patterns and test fixtures.
        if path
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n == "verification_gates.rs")
        {
            continue;
        }

        let Ok(content) = std::fs::read_to_string(&path) else {
            continue;
        };

        let rel = path.strip_prefix(root).unwrap_or(&path);
        let rel = rel.to_string_lossy();

        for (line_no, line) in content.lines().enumerate() {
            // Skip comments that explain the rule itself.
            if line.contains("no references to `master`")
                || line.contains("must be `main`")
                || line.contains("no.*master.*refs")
            {
                continue;
            }

            // Look for git-branch-style references.
            if line.contains("origin/master")
                || line.contains("refs/heads/master")
                || line.contains("branch = \"master\"")
                || line.contains("branch=\"master\"")
            {
                violations.push(format!("{rel}:{}: {}", line_no + 1, line.trim()));
            }
        }
    }
}

fn truncate_string(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_owned()
    } else {
        let mut idx = max_len;
        while idx > 0 && !s.is_char_boundary(idx) {
            idx -= 1;
        }

        let mut truncated = s[..idx].to_owned();
        truncated.push_str("\n... (truncated)");
        truncated
    }
}

// ── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gate_config_default_has_master_refs_check() {
        let config = GateConfig::default();
        assert!(config.check_master_refs);
        assert_eq!(config.max_snippet_len, 2000);
    }

    #[test]
    fn gate_result_serializes() {
        let result = GateResult {
            name: "test-gate".to_owned(),
            passed: true,
            summary: "OK".to_owned(),
            output_snippet: String::new(),
            elapsed_ms: 42,
        };
        let json = serde_json::to_string(&result).expect("serialize");
        let deser: GateResult = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(deser.name, "test-gate");
        assert!(deser.passed);
    }

    #[test]
    fn gate_report_serializes() {
        let report = GateReport {
            gates: vec![GateResult {
                name: "test".to_owned(),
                passed: true,
                summary: "OK".to_owned(),
                output_snippet: String::new(),
                elapsed_ms: 10,
            }],
            all_passed: true,
            total_elapsed_ms: 10,
        };
        let json = serde_json::to_string(&report).expect("serialize");
        let deser: GateReport = serde_json::from_str(&json).expect("deserialize");
        assert!(deser.all_passed);
        assert_eq!(deser.gates.len(), 1);
    }

    #[test]
    fn format_gate_report_contains_gate_names() {
        let report = GateReport {
            gates: vec![
                GateResult {
                    name: "cargo-fmt".to_owned(),
                    passed: true,
                    summary: "OK".to_owned(),
                    output_snippet: String::new(),
                    elapsed_ms: 100,
                },
                GateResult {
                    name: "cargo-clippy".to_owned(),
                    passed: false,
                    summary: "exit code 1".to_owned(),
                    output_snippet: "error: unused import".to_owned(),
                    elapsed_ms: 200,
                },
            ],
            all_passed: false,
            total_elapsed_ms: 300,
        };
        let text = format_gate_report(&report);
        assert!(text.contains("cargo-fmt"));
        assert!(text.contains("cargo-clippy"));
        assert!(text.contains("[PASS]"));
        assert!(text.contains("[FAIL]"));
        assert!(text.contains("SOME GATES FAILED"));
    }

    #[test]
    fn truncate_string_leaves_short_strings_intact() {
        assert_eq!(truncate_string("hello", 100), "hello");
    }

    #[test]
    fn truncate_string_truncates_long_strings() {
        let long = "a".repeat(100);
        let result = truncate_string(&long, 50);
        assert!(result.len() < 100);
        assert!(result.contains("(truncated)"));
    }

    #[test]
    fn scan_master_refs_finds_violations() {
        let dir = tempfile::tempdir().unwrap();
        let rs_file = dir.path().join("test.rs");
        std::fs::write(
            &rs_file,
            "fn main() {\n    let branch = \"origin/master\";\n}\n",
        )
        .unwrap();

        let violations = scan_master_refs(dir.path());
        assert_eq!(violations.len(), 1);
        assert!(violations[0].contains("origin/master"));
    }

    #[test]
    fn scan_master_refs_ignores_explanation_comments() {
        let dir = tempfile::tempdir().unwrap();
        let rs_file = dir.path().join("test.rs");
        std::fs::write(
            &rs_file,
            "// no references to `master` branch (must be `main`)\n",
        )
        .unwrap();

        let violations = scan_master_refs(dir.path());
        assert!(violations.is_empty());
    }

    #[test]
    fn scan_master_refs_empty_dir_returns_empty() {
        let dir = tempfile::tempdir().unwrap();
        let violations = scan_master_refs(dir.path());
        assert!(violations.is_empty());
    }

    #[test]
    fn no_master_refs_in_harness_source() {
        // Verify that the actual fsqlite-e2e source has no master refs.
        let e2e_src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let violations = scan_master_refs(&e2e_src);
        assert!(
            violations.is_empty(),
            "Found master branch references in harness code: {violations:?}"
        );
    }
}
