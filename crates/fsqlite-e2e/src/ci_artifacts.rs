//! CI artifact persistence for regression tracking (bd-2als.6.3).
//!
//! Produces durable, commit-tagged artifact bundles from CI runs so we can:
//!
//! - Track benchmark regressions across commits
//! - Preserve recovery proof bundles for audit
//! - Compare correctness verdicts without re-running locally
//!
//! Artifact naming: `<prefix>_<commit_sha8>_<date>/`
//!
//! ## Layout
//!
//! ```text
//! artifacts/<prefix>_<sha8>_<date>/
//!   manifest.json          — run metadata (commit, date, gates, timings)
//!   bench/
//!     results.jsonl         — raw benchmark run records
//!     summary.md            — rendered speedup curves / p95
//!   correctness/
//!     verdicts.jsonl        — per-fixture tier outcomes
//!   recovery/
//!     proofs/               — per-scenario proof bundles
//!     narrative.md          — walkthrough summary
//! ```

use std::fmt::Write;
use std::path::{Path, PathBuf};

// ── Public types ─────────────────────────────────────────────────────

/// Configuration for artifact generation.
#[derive(Debug, Clone)]
pub struct ArtifactConfig {
    /// Base output directory for artifacts.
    pub output_base: PathBuf,
    /// Artifact directory name prefix.
    pub prefix: String,
    /// Git commit SHA (full or abbreviated).
    pub commit_sha: String,
    /// Run date in `YYYYMMDD` format.
    pub run_date: String,
    /// Whether to include benchmark JSONL.
    pub include_bench: bool,
    /// Whether to include correctness verdicts.
    pub include_correctness: bool,
    /// Whether to include recovery proof bundles.
    pub include_recovery: bool,
}

impl Default for ArtifactConfig {
    fn default() -> Self {
        Self {
            output_base: PathBuf::from("artifacts"),
            prefix: "ci".to_owned(),
            commit_sha: "unknown".to_owned(),
            run_date: "00000000".to_owned(),
            include_bench: true,
            include_correctness: true,
            include_recovery: true,
        }
    }
}

impl ArtifactConfig {
    /// Build the artifact directory name.
    #[must_use]
    pub fn artifact_dir_name(&self) -> String {
        let sha8 = if self.commit_sha.len() >= 8 {
            &self.commit_sha[..8]
        } else {
            &self.commit_sha
        };
        format!("{}_{sha8}_{}", self.prefix, self.run_date)
    }

    /// Full path to the artifact directory.
    #[must_use]
    pub fn artifact_dir(&self) -> PathBuf {
        self.output_base.join(self.artifact_dir_name())
    }
}

/// Manifest describing a CI artifact bundle.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ArtifactManifest {
    /// Git commit SHA.
    pub commit_sha: String,
    /// Run date (`YYYYMMDD`).
    pub run_date: String,
    /// Sections included in this bundle.
    pub sections: Vec<String>,
    /// Total artifact size in bytes (approximate).
    pub total_bytes: u64,
    /// Artifact directory name.
    pub dir_name: String,
}

/// Result of writing an artifact bundle.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ArtifactResult {
    /// Path to the artifact directory.
    pub artifact_dir: String,
    /// Manifest describing the bundle.
    pub manifest: ArtifactManifest,
    /// Whether all sections were written successfully.
    pub success: bool,
    /// Per-section outcomes.
    pub section_outcomes: Vec<SectionOutcome>,
}

/// Outcome of writing a single artifact section.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SectionOutcome {
    /// Section name.
    pub name: String,
    /// Whether the section was written.
    pub written: bool,
    /// Number of files written.
    pub files_written: usize,
    /// Total bytes for this section.
    pub bytes_written: u64,
}

// ── Artifact writer ──────────────────────────────────────────────────

/// Write a CI artifact bundle to disk.
///
/// Creates the directory structure and writes placeholder files for each
/// configured section. In production, the caller provides actual data;
/// this function handles layout and manifest generation.
///
/// # Errors
///
/// Returns `Err` if directory creation or file writing fails.
pub fn write_artifact_bundle(
    config: &ArtifactConfig,
    bench_jsonl: Option<&str>,
    bench_summary: Option<&str>,
    correctness_jsonl: Option<&str>,
    recovery_narrative: Option<&str>,
) -> Result<ArtifactResult, std::io::Error> {
    let artifact_dir = config.artifact_dir();
    std::fs::create_dir_all(&artifact_dir)?;

    let mut sections = Vec::new();
    let mut section_outcomes = Vec::new();
    let mut total_bytes: u64 = 0;

    // ── Bench section ────────────────────────────────────────────
    if config.include_bench {
        let bench_dir = artifact_dir.join("bench");
        std::fs::create_dir_all(&bench_dir)?;
        sections.push("bench".to_owned());

        let mut files_written = 0;
        let mut bytes_written: u64 = 0;

        if let Some(jsonl) = bench_jsonl {
            write_file(&bench_dir.join("results.jsonl"), jsonl)?;
            files_written += 1;
            bytes_written += u64::try_from(jsonl.len()).unwrap_or(0);
        }
        if let Some(summary) = bench_summary {
            write_file(&bench_dir.join("summary.md"), summary)?;
            files_written += 1;
            bytes_written += u64::try_from(summary.len()).unwrap_or(0);
        }

        total_bytes += bytes_written;
        section_outcomes.push(SectionOutcome {
            name: "bench".to_owned(),
            written: true,
            files_written,
            bytes_written,
        });
    }

    // ── Correctness section ──────────────────────────────────────
    if config.include_correctness {
        let correctness_dir = artifact_dir.join("correctness");
        std::fs::create_dir_all(&correctness_dir)?;
        sections.push("correctness".to_owned());

        let mut files_written = 0;
        let mut bytes_written: u64 = 0;

        if let Some(jsonl) = correctness_jsonl {
            write_file(&correctness_dir.join("verdicts.jsonl"), jsonl)?;
            files_written += 1;
            bytes_written += u64::try_from(jsonl.len()).unwrap_or(0);
        }

        total_bytes += bytes_written;
        section_outcomes.push(SectionOutcome {
            name: "correctness".to_owned(),
            written: true,
            files_written,
            bytes_written,
        });
    }

    // ── Recovery section ─────────────────────────────────────────
    if config.include_recovery {
        let recovery_dir = artifact_dir.join("recovery");
        std::fs::create_dir_all(&recovery_dir)?;
        std::fs::create_dir_all(recovery_dir.join("proofs"))?;
        sections.push("recovery".to_owned());

        let mut files_written = 0;
        let mut bytes_written: u64 = 0;

        if let Some(narrative) = recovery_narrative {
            write_file(&recovery_dir.join("narrative.md"), narrative)?;
            files_written += 1;
            bytes_written += u64::try_from(narrative.len()).unwrap_or(0);
        }

        total_bytes += bytes_written;
        section_outcomes.push(SectionOutcome {
            name: "recovery".to_owned(),
            written: true,
            files_written,
            bytes_written,
        });
    }

    // ── Manifest ─────────────────────────────────────────────────
    let manifest = ArtifactManifest {
        commit_sha: config.commit_sha.clone(),
        run_date: config.run_date.clone(),
        sections,
        total_bytes,
        dir_name: config.artifact_dir_name(),
    };

    let manifest_json = serde_json::to_string_pretty(&manifest)
        .map_err(|e| std::io::Error::other(format!("manifest serialization: {e}")))?;
    write_file(&artifact_dir.join("manifest.json"), &manifest_json)?;

    Ok(ArtifactResult {
        artifact_dir: artifact_dir.to_string_lossy().to_string(),
        manifest,
        success: true,
        section_outcomes,
    })
}

/// Format an artifact result as a human-readable summary.
#[must_use]
pub fn format_artifact_result(result: &ArtifactResult) -> String {
    let mut out = String::with_capacity(512);

    let _ = writeln!(out, "=== CI Artifact Bundle ===");
    let _ = writeln!(out, "  Dir: {}", result.artifact_dir);
    let _ = writeln!(out, "  Commit: {}", result.manifest.commit_sha);
    let _ = writeln!(out, "  Date: {}", result.manifest.run_date);
    let _ = writeln!(out);

    for section in &result.section_outcomes {
        let status = if section.written { "OK" } else { "SKIP" };
        let _ = writeln!(
            out,
            "  [{status}] {} — {} files, {} bytes",
            section.name, section.files_written, section.bytes_written
        );
    }

    let _ = writeln!(out);
    let _ = writeln!(
        out,
        "  Total: {} bytes across {} sections",
        result.manifest.total_bytes,
        result.manifest.sections.len()
    );

    out
}

// ── Helpers ──────────────────────────────────────────────────────────

fn write_file(path: &Path, content: &str) -> Result<(), std::io::Error> {
    std::fs::write(path, content)
}

// ── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config(dir: &Path) -> ArtifactConfig {
        ArtifactConfig {
            output_base: dir.to_path_buf(),
            prefix: "test".to_owned(),
            commit_sha: "abc12345def67890".to_owned(),
            run_date: "20260210".to_owned(),
            ..ArtifactConfig::default()
        }
    }

    #[test]
    fn artifact_dir_name_uses_sha8() {
        let config = ArtifactConfig {
            commit_sha: "abc12345def67890".to_owned(),
            run_date: "20260210".to_owned(),
            prefix: "ci".to_owned(),
            ..ArtifactConfig::default()
        };
        assert_eq!(config.artifact_dir_name(), "ci_abc12345_20260210");
    }

    #[test]
    fn artifact_dir_name_short_sha() {
        let config = ArtifactConfig {
            commit_sha: "abc".to_owned(),
            run_date: "20260210".to_owned(),
            prefix: "ci".to_owned(),
            ..ArtifactConfig::default()
        };
        assert_eq!(config.artifact_dir_name(), "ci_abc_20260210");
    }

    #[test]
    fn write_artifact_bundle_creates_structure() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());

        let result = write_artifact_bundle(
            &config,
            Some("{\"fixture\":\"test\",\"throughput\":1000}\n"),
            Some("# Benchmark Summary\n\nSpeedup: 2.5x\n"),
            Some("{\"fixture\":\"test\",\"tier\":1,\"passed\":true}\n"),
            Some("# Recovery Narrative\n\nAll scenarios passed.\n"),
        )
        .unwrap();

        assert!(result.success);
        assert_eq!(result.section_outcomes.len(), 3);

        // Verify directory structure.
        let art_dir = config.artifact_dir();
        assert!(art_dir.join("manifest.json").exists());
        assert!(art_dir.join("bench/results.jsonl").exists());
        assert!(art_dir.join("bench/summary.md").exists());
        assert!(art_dir.join("correctness/verdicts.jsonl").exists());
        assert!(art_dir.join("recovery/narrative.md").exists());
        assert!(art_dir.join("recovery/proofs").is_dir());
    }

    #[test]
    fn write_artifact_bundle_respects_section_flags() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.include_bench = false;
        config.include_recovery = false;

        let result =
            write_artifact_bundle(&config, None, None, Some("verdicts here\n"), None).unwrap();

        assert!(result.success);
        assert_eq!(result.section_outcomes.len(), 1);
        assert_eq!(result.section_outcomes[0].name, "correctness");

        let art_dir = config.artifact_dir();
        assert!(!art_dir.join("bench").exists());
        assert!(art_dir.join("correctness/verdicts.jsonl").exists());
        assert!(!art_dir.join("recovery").exists());
    }

    #[test]
    fn manifest_serialization_roundtrip() {
        let manifest = ArtifactManifest {
            commit_sha: "abc12345".to_owned(),
            run_date: "20260210".to_owned(),
            sections: vec!["bench".to_owned(), "recovery".to_owned()],
            total_bytes: 4096,
            dir_name: "ci_abc12345_20260210".to_owned(),
        };

        let json = serde_json::to_string(&manifest).unwrap();
        let deser: ArtifactManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(deser.commit_sha, "abc12345");
        assert_eq!(deser.sections.len(), 2);
        assert_eq!(deser.total_bytes, 4096);
    }

    #[test]
    fn artifact_result_serialization_roundtrip() {
        let result = ArtifactResult {
            artifact_dir: "/tmp/test".to_owned(),
            manifest: ArtifactManifest {
                commit_sha: "abc".to_owned(),
                run_date: "20260210".to_owned(),
                sections: vec!["bench".to_owned()],
                total_bytes: 100,
                dir_name: "test".to_owned(),
            },
            success: true,
            section_outcomes: vec![SectionOutcome {
                name: "bench".to_owned(),
                written: true,
                files_written: 2,
                bytes_written: 100,
            }],
        };

        let json = serde_json::to_string(&result).unwrap();
        let deser: ArtifactResult = serde_json::from_str(&json).unwrap();
        assert!(deser.success);
        assert_eq!(deser.section_outcomes.len(), 1);
    }

    #[test]
    fn format_artifact_result_contains_sections() {
        let result = ArtifactResult {
            artifact_dir: "/tmp/ci_abc12345_20260210".to_owned(),
            manifest: ArtifactManifest {
                commit_sha: "abc12345".to_owned(),
                run_date: "20260210".to_owned(),
                sections: vec!["bench".to_owned(), "recovery".to_owned()],
                total_bytes: 5000,
                dir_name: "ci_abc12345_20260210".to_owned(),
            },
            success: true,
            section_outcomes: vec![
                SectionOutcome {
                    name: "bench".to_owned(),
                    written: true,
                    files_written: 2,
                    bytes_written: 3000,
                },
                SectionOutcome {
                    name: "recovery".to_owned(),
                    written: true,
                    files_written: 1,
                    bytes_written: 2000,
                },
            ],
        };

        let text = format_artifact_result(&result);
        assert!(text.contains("bench"));
        assert!(text.contains("recovery"));
        assert!(text.contains("abc12345"));
        assert!(text.contains("5000"));
    }

    #[test]
    fn default_config_includes_all_sections() {
        let config = ArtifactConfig::default();
        assert!(config.include_bench);
        assert!(config.include_correctness);
        assert!(config.include_recovery);
    }

    #[test]
    fn write_bundle_with_no_data_creates_empty_sections() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());

        let result = write_artifact_bundle(&config, None, None, None, None).unwrap();

        assert!(result.success);
        // Sections still created even without data.
        assert_eq!(result.section_outcomes.len(), 3);
        for outcome in &result.section_outcomes {
            assert_eq!(outcome.files_written, 0);
            assert_eq!(outcome.bytes_written, 0);
        }
    }
}
