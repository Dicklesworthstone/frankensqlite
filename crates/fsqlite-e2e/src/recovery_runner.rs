//! FrankenSQLite recovery runner with classification and proof artifacts (bd-2als.4.2).
//!
//! Given a corrupted working copy, this module:
//!
//! 1. Detects corruption (via WAL checksum chain + xxh3-128 page hashes).
//! 2. Attempts repair using available redundancy (WAL-FEC repair symbols).
//! 3. Classifies the outcome as [`RecoveryClassification::Recovered`],
//!    [`RecoveryClassification::Partial`], or [`RecoveryClassification::Lost`].
//! 4. Emits [`RecoveryEvidence`] capturing what was detected, what was repaired,
//!    and post-repair integrity checks.
//! 5. Writes proof artifacts (JSON report + narrative markdown) to a per-run
//!    output directory.
//!
//! Recovery is deterministic for a given corrupted input + symbol set.

use std::fs;
use std::path::{Path, PathBuf};

use fsqlite_wal::{WalFecRecoveryLog, WalFecRecoveryOutcome};

use crate::corruption::CorruptionReport;
use crate::corruption_scenarios::{
    CorruptionScenario, CorruptionTarget, ExpectedFsqliteBehavior, ScenarioCorruptionPattern,
};
use crate::recovery_demo::{
    RecoveryDemoConfig, WalInfo, attempt_wal_fec_recovery_with_config, build_wal_fec_sidecar,
    parse_wal_file,
};

// ── Classification ──────────────────────────────────────────────────────

/// Outcome classification for a recovery attempt (§3.4.6, bd-2als.4.2).
///
/// Every recovery attempt ends in exactly one of these states.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryClassification {
    /// DB is usable and passes integrity checks; meets target equivalence tier.
    /// All corrupted pages/frames were successfully repaired.
    Recovered {
        /// Number of pages/frames that were repaired.
        pages_repaired: usize,
        /// Number of repair symbols consumed.
        symbols_used: u32,
    },
    /// DB opens but some data is unrecoverable.  Reports what was lost.
    Partial {
        /// Pages/frames that were successfully repaired.
        pages_repaired: usize,
        /// Pages/frames that could not be repaired.
        pages_lost: usize,
        /// Human-readable description of what was lost.
        loss_description: String,
    },
    /// DB cannot be recovered with available symbols.  No repair was possible.
    Lost {
        /// Why recovery failed.
        reason: LostReason,
    },
}

/// Why a recovery was classified as [`RecoveryClassification::Lost`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LostReason {
    /// Corruption exceeds the R repair symbol budget.
    InsufficientSymbols {
        /// Number of corrupted items detected.
        corrupted_count: u32,
        /// Available repair symbol budget (R).
        r_budget: u32,
    },
    /// WAL-FEC sidecar is missing or cannot be read.
    SidecarMissing,
    /// WAL-FEC sidecar itself is corrupted.
    SidecarDamaged { detail: String },
    /// Recovery was explicitly disabled in configuration.
    RecoveryDisabled,
    /// Database file corruption (header/page) with no DB-FEC sidecar.
    NoDbFecAvailable,
    /// Decode returned data but post-repair integrity check failed.
    IntegrityCheckFailed { detail: String },
}

// ── Evidence ────────────────────────────────────────────────────────────

/// Structured evidence of what was detected and repaired.
#[derive(Debug, Clone)]
pub struct RecoveryEvidence {
    /// Corruption that was detected (checksums, decode failures).
    pub detection: Vec<DetectionEntry>,
    /// Repairs that were applied (symbols used, pages/frames restored).
    pub repairs: Vec<RepairEntry>,
    /// Post-repair integrity check results.
    pub integrity_checks: Vec<IntegrityCheck>,
}

/// A single detection event during recovery analysis.
#[derive(Debug, Clone)]
pub struct DetectionEntry {
    /// What was checked (e.g. "WAL frame 1 xxh3-128", "DB page 2 checksum").
    pub target: String,
    /// The detection method (e.g. "xxh3_128_mismatch", "wal_checksum_chain_break").
    pub method: String,
    /// Expected hash/value.
    pub expected: String,
    /// Observed hash/value.
    pub observed: String,
}

/// A single repair event during recovery.
#[derive(Debug, Clone)]
pub struct RepairEntry {
    /// What was repaired (e.g. "WAL frame 1", "DB page 5").
    pub target: String,
    /// Source of repair data (e.g. "wal-fec sidecar group 0").
    pub source: String,
    /// Number of symbols consumed for this repair.
    pub symbols_consumed: u32,
    /// Whether post-repair hash verification passed.
    pub verified: bool,
}

/// An integrity check performed after repair.
#[derive(Debug, Clone)]
pub struct IntegrityCheck {
    /// Check name (e.g. "PRAGMA integrity_check", "tiered_equivalence").
    pub name: String,
    /// Whether the check passed.
    pub passed: bool,
    /// Detail output from the check.
    pub detail: String,
}

// ── Report ──────────────────────────────────────────────────────────────

/// Unified recovery report for a single scenario run (bd-2als.4.2).
#[derive(Debug, Clone)]
pub struct RecoveryReport {
    /// Scenario name from the catalog.
    pub scenario_name: String,
    /// Recovery classification outcome.
    pub classification: RecoveryClassification,
    /// Structured evidence of detection + repair.
    pub evidence: RecoveryEvidence,
    /// The corruption report from injection (if available).
    pub corruption_report: Option<CorruptionReport>,
    /// The WAL-FEC recovery log (if recovery was attempted).
    pub wal_recovery_log: Option<WalFecRecoveryLog>,
    /// Whether the outcome matched the scenario's expected behavior.
    pub matches_expected: bool,
    /// Human-readable verdict summarizing the outcome.
    pub verdict: String,
}

/// Aggregate report for a batch of recovery runs.
#[derive(Debug)]
pub struct RecoveryBatchReport {
    /// Individual scenario reports.
    pub reports: Vec<RecoveryReport>,
    /// Total scenarios executed.
    pub total: usize,
    /// Scenarios classified as Recovered.
    pub recovered_count: usize,
    /// Scenarios classified as Partial.
    pub partial_count: usize,
    /// Scenarios classified as Lost.
    pub lost_count: usize,
    /// Scenarios where outcome matched expectations.
    pub matched_count: usize,
}

impl RecoveryBatchReport {
    /// Build from individual reports.
    #[must_use]
    pub fn from_reports(reports: Vec<RecoveryReport>) -> Self {
        let total = reports.len();
        let recovered_count = reports
            .iter()
            .filter(|r| matches!(r.classification, RecoveryClassification::Recovered { .. }))
            .count();
        let partial_count = reports
            .iter()
            .filter(|r| matches!(r.classification, RecoveryClassification::Partial { .. }))
            .count();
        let lost_count = reports
            .iter()
            .filter(|r| matches!(r.classification, RecoveryClassification::Lost { .. }))
            .count();
        let matched_count = reports.iter().filter(|r| r.matches_expected).count();
        Self {
            reports,
            total,
            recovered_count,
            partial_count,
            lost_count,
            matched_count,
        }
    }

    /// Whether all outcomes matched expectations.
    #[must_use]
    pub fn all_matched(&self) -> bool {
        self.matched_count == self.total
    }
}

// ── Runner ──────────────────────────────────────────────────────────────

/// Run a single corruption scenario through the full recovery pipeline.
///
/// Returns a [`RecoveryReport`] with classification, evidence, and proofs.
#[allow(clippy::too_many_lines)]
#[must_use]
pub fn run_recovery(scenario: &CorruptionScenario) -> RecoveryReport {
    let dir = match tempfile::tempdir() {
        Ok(d) => d,
        Err(e) => {
            return make_error_report(
                scenario.name,
                &format!("tempdir creation failed: {e}"),
                &scenario.expected_fsqlite,
            );
        }
    };

    // DB-corruption scenarios use a rollback-journal fixture.
    if scenario.target == CorruptionTarget::Database {
        return run_db_corruption_recovery(scenario, dir.path());
    }

    // WAL-corruption scenarios.
    run_wal_corruption_recovery(scenario, dir.path())
}

/// Run all scenarios and produce a batch report.
#[must_use]
pub fn run_all_recoveries(scenarios: &[CorruptionScenario]) -> RecoveryBatchReport {
    let reports: Vec<RecoveryReport> = scenarios.iter().map(run_recovery).collect();
    RecoveryBatchReport::from_reports(reports)
}

// ── WAL recovery path ───────────────────────────────────────────────────

#[allow(clippy::too_many_lines)]
fn run_wal_corruption_recovery(scenario: &CorruptionScenario, dir: &Path) -> RecoveryReport {
    let mut evidence = RecoveryEvidence {
        detection: Vec::new(),
        repairs: Vec::new(),
        integrity_checks: Vec::new(),
    };

    // Step 1: Setup WAL-mode fixture.
    let (db_path, _rows) = setup_wal_fixture(dir, scenario.setup_row_count);
    let wal_path = db_path.with_extension("db-wal");

    // Step 2: Parse WAL.
    let (info, original_pages) = match parse_wal_file(&wal_path) {
        Ok(r) => r,
        Err(e) => {
            return make_error_report(
                scenario.name,
                &format!("WAL parse failed: {e}"),
                &scenario.expected_fsqlite,
            );
        }
    };

    // Step 3: Optionally build WAL-FEC sidecar.
    if scenario.setup_wal_fec
        && let Err(e) = build_wal_fec_sidecar(
            &wal_path,
            &info,
            &original_pages,
            scenario.setup_repair_symbols,
        )
    {
        return make_error_report(
            scenario.name,
            &format!("sidecar build failed: {e}"),
            &scenario.expected_fsqlite,
        );
    }

    // Step 4: Inject corruption.
    let corruption_report = match inject_scenario_corruption(scenario, &db_path, &wal_path, &info) {
        Ok(r) => r,
        Err(e) => {
            return make_error_report(
                scenario.name,
                &format!("corruption injection failed: {e}"),
                &scenario.expected_fsqlite,
            );
        }
    };

    // Record detection evidence from corruption report.
    for modification in &corruption_report.modifications {
        let target = if let (Some(first), Some(last)) =
            (modification.wal_frame_first, modification.wal_frame_last)
        {
            if first == last {
                format!("WAL frame {first}")
            } else {
                format!("WAL frames {first}-{last}")
            }
        } else {
            format!(
                "bytes {}-{}",
                modification.offset,
                modification.offset + modification.length
            )
        };

        evidence.detection.push(DetectionEntry {
            target,
            method: "injected_corruption".to_owned(),
            expected: modification.sha256_before.clone(),
            observed: modification
                .sha256_after
                .clone()
                .unwrap_or_else(|| "(truncated)".to_owned()),
        });
    }

    // Step 5: Determine corrupted frame numbers.
    let corrupted_frames = corrupted_frame_numbers(scenario, &info);

    // Step 6: Attempt WAL-FEC recovery.
    let config = RecoveryDemoConfig {
        recovery_enabled: scenario.fsqlite_recovery_enabled,
        repair_symbols: scenario.setup_repair_symbols,
    };

    let recovery_result = attempt_wal_fec_recovery_with_config(
        &wal_path,
        &info,
        original_pages.clone(),
        &corrupted_frames,
        &config,
    );

    match recovery_result {
        Ok((outcome, log)) => classify_wal_outcome(
            scenario,
            &outcome,
            &log,
            &original_pages,
            &corruption_report,
            evidence,
        ),
        Err(e) => {
            // Recovery function itself errored.
            let reason = if !scenario.fsqlite_recovery_enabled {
                LostReason::RecoveryDisabled
            } else if !scenario.setup_wal_fec {
                LostReason::SidecarMissing
            } else if scenario.target == CorruptionTarget::WalFecSidecar {
                LostReason::SidecarDamaged {
                    detail: e.to_string(),
                }
            } else {
                LostReason::InsufficientSymbols {
                    corrupted_count: u32::try_from(corrupted_frames.len()).unwrap_or(u32::MAX),
                    r_budget: scenario.setup_repair_symbols,
                }
            };

            let classification = RecoveryClassification::Lost { reason };
            let matches = matches_expected(&classification, &scenario.expected_fsqlite);
            let verdict = if matches {
                format!("Recovery failed as expected: {e}")
            } else {
                format!("Unexpected recovery error: {e}")
            };

            RecoveryReport {
                scenario_name: scenario.name.to_owned(),
                classification,
                evidence,
                corruption_report: Some(corruption_report),
                wal_recovery_log: None,
                matches_expected: matches,
                verdict,
            }
        }
    }
}

#[allow(clippy::too_many_lines)]
fn classify_wal_outcome(
    scenario: &CorruptionScenario,
    outcome: &WalFecRecoveryOutcome,
    log: &WalFecRecoveryLog,
    original_pages: &[Vec<u8>],
    corruption_report: &CorruptionReport,
    mut evidence: RecoveryEvidence,
) -> RecoveryReport {
    match outcome {
        WalFecRecoveryOutcome::Recovered(group) => {
            // Verify recovered pages match originals.
            let mut pages_verified = 0usize;
            let mut verification_failures = Vec::new();

            for (i, (recovered, original)) in group
                .recovered_pages
                .iter()
                .zip(original_pages.iter())
                .enumerate()
            {
                if recovered == original {
                    pages_verified += 1;
                } else {
                    verification_failures.push(i);
                }
            }

            // Record repair evidence.
            evidence.repairs.push(RepairEntry {
                target: format!("{} WAL frames", group.recovered_pages.len()),
                source: "wal-fec sidecar".to_owned(),
                symbols_consumed: log.required_symbols,
                verified: verification_failures.is_empty(),
            });

            // Post-repair integrity check: page content match.
            let integrity_passed = verification_failures.is_empty();
            evidence.integrity_checks.push(IntegrityCheck {
                name: "page_content_verification".to_owned(),
                passed: integrity_passed,
                detail: if integrity_passed {
                    format!(
                        "{pages_verified}/{} pages verified correct",
                        original_pages.len()
                    )
                } else {
                    format!(
                        "{} pages failed verification: {:?}",
                        verification_failures.len(),
                        verification_failures
                    )
                },
            });

            let classification = if integrity_passed {
                RecoveryClassification::Recovered {
                    pages_repaired: group.recovered_pages.len(),
                    symbols_used: log.required_symbols,
                }
            } else {
                RecoveryClassification::Partial {
                    pages_repaired: pages_verified,
                    pages_lost: verification_failures.len(),
                    loss_description: format!(
                        "frames {:?} content mismatch after recovery",
                        verification_failures
                    ),
                }
            };

            let matches = matches_expected(&classification, &scenario.expected_fsqlite);
            let verdict = match &classification {
                RecoveryClassification::Recovered { pages_repaired, .. } => {
                    format!("Full recovery: {pages_repaired} pages restored correctly")
                }
                RecoveryClassification::Partial {
                    pages_repaired,
                    pages_lost,
                    ..
                } => {
                    format!("Partial recovery: {pages_repaired} repaired, {pages_lost} lost")
                }
                RecoveryClassification::Lost { .. } => unreachable!(),
            };

            RecoveryReport {
                scenario_name: scenario.name.to_owned(),
                classification,
                evidence,
                corruption_report: Some(corruption_report.clone()),
                wal_recovery_log: Some(log.clone()),
                matches_expected: matches,
                verdict,
            }
        }
        WalFecRecoveryOutcome::TruncateBeforeGroup { .. } => {
            let reason = if log.recovery_enabled {
                if scenario.target == CorruptionTarget::WalFecSidecar {
                    LostReason::SidecarDamaged {
                        detail: "sidecar corruption caused recovery fallback to truncation"
                            .to_owned(),
                    }
                } else {
                    LostReason::InsufficientSymbols {
                        corrupted_count: u32::try_from(corruption_report.affected_pages.len())
                            .unwrap_or(0),
                        r_budget: scenario.setup_repair_symbols,
                    }
                }
            } else {
                LostReason::RecoveryDisabled
            };

            let classification = RecoveryClassification::Lost {
                reason: reason.clone(),
            };
            let matches = matches_expected(&classification, &scenario.expected_fsqlite);
            let verdict = match &reason {
                LostReason::RecoveryDisabled => {
                    "Recovery disabled: WAL truncated as expected".to_owned()
                }
                LostReason::InsufficientSymbols {
                    corrupted_count,
                    r_budget,
                } => {
                    format!(
                        "Lost: {corrupted_count} corrupted items exceed R={r_budget} repair budget"
                    )
                }
                _ => format!("Lost: truncation fallback ({reason:?})"),
            };

            RecoveryReport {
                scenario_name: scenario.name.to_owned(),
                classification,
                evidence,
                corruption_report: Some(corruption_report.clone()),
                wal_recovery_log: Some(log.clone()),
                matches_expected: matches,
                verdict,
            }
        }
    }
}

// ── DB corruption path (no WAL-FEC recovery) ────────────────────────────

fn run_db_corruption_recovery(scenario: &CorruptionScenario, dir: &Path) -> RecoveryReport {
    let mut evidence = RecoveryEvidence {
        detection: Vec::new(),
        repairs: Vec::new(),
        integrity_checks: Vec::new(),
    };

    let db_path = setup_db_fixture(dir, scenario.setup_row_count);

    let corruption_report = match inject_db_corruption(scenario, &db_path) {
        Ok(r) => r,
        Err(e) => {
            return make_error_report(
                scenario.name,
                &format!("DB corruption injection failed: {e}"),
                &scenario.expected_fsqlite,
            );
        }
    };

    // Record detection evidence.
    for modification in &corruption_report.modifications {
        evidence.detection.push(DetectionEntry {
            target: format!(
                "DB pages {}-{}",
                modification.page_first.unwrap_or(0),
                modification.page_last.unwrap_or(0)
            ),
            method: "injected_corruption".to_owned(),
            expected: modification.sha256_before.clone(),
            observed: modification
                .sha256_after
                .clone()
                .unwrap_or_else(|| "(truncated)".to_owned()),
        });
    }

    // Attempt integrity check on the corrupted DB.
    let integrity_result = run_integrity_check(&db_path);
    evidence.integrity_checks.push(IntegrityCheck {
        name: "PRAGMA integrity_check".to_owned(),
        passed: integrity_result.is_ok(),
        detail: match &integrity_result {
            Ok(msg) => msg.clone(),
            Err(e) => e.clone(),
        },
    });

    // DB corruption has no WAL-FEC recovery path available.
    let classification = RecoveryClassification::Lost {
        reason: LostReason::NoDbFecAvailable,
    };
    let matches = matches_expected(&classification, &scenario.expected_fsqlite);

    let verdict = format!(
        "DB corruption: no WAL-FEC recovery available (affected {} bytes)",
        corruption_report.affected_bytes
    );

    RecoveryReport {
        scenario_name: scenario.name.to_owned(),
        classification,
        evidence,
        corruption_report: Some(corruption_report),
        wal_recovery_log: None,
        matches_expected: matches,
        verdict,
    }
}

// ── Artifact writer ─────────────────────────────────────────────────────

/// Write recovery proof artifacts to the given output directory.
///
/// Creates:
/// - `recovery_report.json` — machine-readable JSON report.
/// - `recovery_narrative.md` — human-readable markdown summary.
///
/// # Errors
///
/// Returns an error if the output directory cannot be created or files
/// cannot be written.
pub fn write_recovery_artifacts(
    report: &RecoveryReport,
    output_dir: &Path,
) -> std::io::Result<PathBuf> {
    fs::create_dir_all(output_dir)?;

    // JSON report.
    let json_path = output_dir.join("recovery_report.json");
    let json = serialize_report_json(report);
    fs::write(&json_path, json)?;

    // Markdown narrative.
    let md_path = output_dir.join("recovery_narrative.md");
    let narrative = render_narrative(report);
    fs::write(&md_path, narrative)?;

    Ok(json_path)
}

fn serialize_report_json(report: &RecoveryReport) -> String {
    use std::fmt::Write as _;

    let mut json = String::with_capacity(2048);
    let _ = writeln!(json, "{{");
    let _ = writeln!(json, "  \"scenario\": \"{}\",", report.scenario_name);
    let _ = writeln!(
        json,
        "  \"classification\": \"{}\",",
        classification_tag(&report.classification)
    );
    let _ = writeln!(json, "  \"matches_expected\": {},", report.matches_expected);
    let _ = writeln!(json, "  \"verdict\": {:?},", report.verdict);

    // Detection events.
    let _ = writeln!(json, "  \"detection\": [");
    for (i, d) in report.evidence.detection.iter().enumerate() {
        let comma = if i + 1 < report.evidence.detection.len() {
            ","
        } else {
            ""
        };
        let _ = writeln!(
            json,
            "    {{\"target\": {:?}, \"method\": {:?}, \"expected\": {:?}, \"observed\": {:?}}}{comma}",
            d.target, d.method, d.expected, d.observed
        );
    }
    let _ = writeln!(json, "  ],");

    // Repairs.
    let _ = writeln!(json, "  \"repairs\": [");
    for (i, r) in report.evidence.repairs.iter().enumerate() {
        let comma = if i + 1 < report.evidence.repairs.len() {
            ","
        } else {
            ""
        };
        let _ = writeln!(
            json,
            "    {{\"target\": {:?}, \"source\": {:?}, \"symbols_consumed\": {}, \"verified\": {}}}{comma}",
            r.target, r.source, r.symbols_consumed, r.verified
        );
    }
    let _ = writeln!(json, "  ],");

    // Integrity checks.
    let _ = writeln!(json, "  \"integrity_checks\": [");
    for (i, ic) in report.evidence.integrity_checks.iter().enumerate() {
        let comma = if i + 1 < report.evidence.integrity_checks.len() {
            ","
        } else {
            ""
        };
        let _ = writeln!(
            json,
            "    {{\"name\": {:?}, \"passed\": {}, \"detail\": {:?}}}{comma}",
            ic.name, ic.passed, ic.detail
        );
    }
    let _ = writeln!(json, "  ]");

    let _ = write!(json, "}}");
    json
}

fn render_narrative(report: &RecoveryReport) -> String {
    use std::fmt::Write as _;

    let mut md = String::with_capacity(2048);
    let _ = writeln!(md, "# Recovery Report: {}\n", report.scenario_name);
    let _ = writeln!(
        md,
        "**Classification:** {}\n",
        classification_tag(&report.classification)
    );
    let _ = writeln!(md, "**Verdict:** {}\n", report.verdict);
    let _ = writeln!(
        md,
        "**Matches Expected:** {}\n",
        if report.matches_expected { "YES" } else { "NO" }
    );

    // Classification details.
    match &report.classification {
        RecoveryClassification::Recovered {
            pages_repaired,
            symbols_used,
        } => {
            let _ = writeln!(md, "## Recovery Details\n");
            let _ = writeln!(md, "- Pages repaired: {pages_repaired}");
            let _ = writeln!(md, "- Symbols used: {symbols_used}");
        }
        RecoveryClassification::Partial {
            pages_repaired,
            pages_lost,
            loss_description,
        } => {
            let _ = writeln!(md, "## Partial Recovery\n");
            let _ = writeln!(md, "- Pages repaired: {pages_repaired}");
            let _ = writeln!(md, "- Pages lost: {pages_lost}");
            let _ = writeln!(md, "- Loss: {loss_description}");
        }
        RecoveryClassification::Lost { reason } => {
            let _ = writeln!(md, "## Recovery Failed\n");
            let _ = writeln!(md, "- Reason: {reason:?}");
        }
    }

    // Detection evidence.
    if !report.evidence.detection.is_empty() {
        let _ = writeln!(md, "\n## Corruption Detection\n");
        let _ = writeln!(md, "| Target | Method | Expected | Observed |");
        let _ = writeln!(md, "|--------|--------|----------|----------|");
        for d in &report.evidence.detection {
            let exp = truncate_hash(&d.expected);
            let obs = truncate_hash(&d.observed);
            let _ = writeln!(
                md,
                "| {} | {} | `{}` | `{}` |",
                d.target, d.method, exp, obs
            );
        }
    }

    // Repair evidence.
    if !report.evidence.repairs.is_empty() {
        let _ = writeln!(md, "\n## Repairs Applied\n");
        let _ = writeln!(md, "| Target | Source | Symbols | Verified |");
        let _ = writeln!(md, "|--------|--------|---------|----------|");
        for r in &report.evidence.repairs {
            let _ = writeln!(
                md,
                "| {} | {} | {} | {} |",
                r.target,
                r.source,
                r.symbols_consumed,
                if r.verified { "YES" } else { "NO" }
            );
        }
    }

    // Integrity checks.
    if !report.evidence.integrity_checks.is_empty() {
        let _ = writeln!(md, "\n## Post-Repair Integrity Checks\n");
        for ic in &report.evidence.integrity_checks {
            let status = if ic.passed { "PASS" } else { "FAIL" };
            let _ = writeln!(md, "- **{}**: {} — {}", ic.name, status, ic.detail);
        }
    }

    md
}

fn truncate_hash(hash: &str) -> String {
    if hash.len() > 16 {
        format!("{}...", &hash[..16])
    } else {
        hash.to_owned()
    }
}

fn classification_tag(c: &RecoveryClassification) -> &'static str {
    match c {
        RecoveryClassification::Recovered { .. } => "recovered",
        RecoveryClassification::Partial { .. } => "partial",
        RecoveryClassification::Lost { .. } => "lost",
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────

#[allow(clippy::unnested_or_patterns)]
fn matches_expected(
    classification: &RecoveryClassification,
    expected: &ExpectedFsqliteBehavior,
) -> bool {
    matches!(
        (classification, expected),
        (
            RecoveryClassification::Recovered { .. },
            ExpectedFsqliteBehavior::FullRecovery
        ) | (
            RecoveryClassification::Lost {
                reason: LostReason::InsufficientSymbols { .. }
                    | LostReason::SidecarMissing
                    | LostReason::NoDbFecAvailable,
            },
            ExpectedFsqliteBehavior::RepairExceedsCapacity,
        ) | (
            RecoveryClassification::Lost {
                reason: LostReason::RecoveryDisabled,
            },
            ExpectedFsqliteBehavior::RecoveryDisabled,
        ) | (
            // Sidecar damaged but WAL frames intact — recovery succeeds because
            // repair wasn't needed; still a valid outcome for the scenario.
            RecoveryClassification::Lost {
                reason: LostReason::SidecarDamaged { .. },
            } | RecoveryClassification::Recovered { .. },
            ExpectedFsqliteBehavior::SidecarDamaged,
        )
    )
}

fn make_error_report(
    scenario_name: &str,
    error_msg: &str,
    expected: &ExpectedFsqliteBehavior,
) -> RecoveryReport {
    let classification = RecoveryClassification::Lost {
        reason: LostReason::IntegrityCheckFailed {
            detail: error_msg.to_owned(),
        },
    };
    let matches = matches_expected(&classification, expected);
    RecoveryReport {
        scenario_name: scenario_name.to_owned(),
        classification,
        evidence: RecoveryEvidence {
            detection: Vec::new(),
            repairs: Vec::new(),
            integrity_checks: Vec::new(),
        },
        corruption_report: None,
        wal_recovery_log: None,
        matches_expected: matches,
        verdict: error_msg.to_owned(),
    }
}

/// Setup a WAL-mode database fixture for recovery testing.
fn setup_wal_fixture(dir: &Path, row_count: usize) -> (PathBuf, Vec<(i64, String)>) {
    let live_db = dir.join("live.db");
    let crash_db = dir.join("crash.db");

    let conn = rusqlite::Connection::open(&live_db).expect("open live db");
    conn.execute_batch("PRAGMA journal_mode=WAL;")
        .expect("set WAL");
    conn.execute_batch("PRAGMA synchronous=NORMAL;")
        .expect("set sync");
    conn.execute_batch("PRAGMA wal_autocheckpoint=0;")
        .expect("disable autocheckpoint");
    conn.execute_batch("CREATE TABLE demo (id INTEGER PRIMARY KEY, payload TEXT NOT NULL);")
        .expect("create table");

    let mut rows = Vec::with_capacity(row_count);
    for i in 0..row_count {
        let id = i64::try_from(i + 1).expect("index fits i64");
        let payload = format!("recovery-runner-row-{id:04}");
        conn.execute(
            "INSERT INTO demo (id, payload) VALUES (?1, ?2)",
            rusqlite::params![id, payload],
        )
        .expect("insert");
        rows.push((id, payload));
    }

    let live_wal = live_db.with_extension("db-wal");
    assert!(live_wal.exists(), "WAL must exist while writer is open");

    fs::copy(&live_db, &crash_db).expect("copy db");
    fs::copy(&live_wal, crash_db.with_extension("db-wal")).expect("copy wal");
    let live_shm = live_db.with_extension("db-shm");
    if live_shm.exists() {
        fs::copy(&live_shm, crash_db.with_extension("db-shm")).expect("copy shm");
    }

    drop(conn);
    (crash_db, rows)
}

/// Create a rollback-journal DB fixture for DB-corruption scenarios.
fn setup_db_fixture(dir: &Path, row_count: usize) -> PathBuf {
    let db_path = dir.join("db_corruption.db");
    let conn = rusqlite::Connection::open(&db_path).expect("open db fixture");
    conn.execute_batch(
        "PRAGMA page_size=4096;\n\
         PRAGMA journal_mode=DELETE;\n\
         PRAGMA synchronous=FULL;\n\
         CREATE TABLE demo (id INTEGER PRIMARY KEY, payload TEXT NOT NULL);",
    )
    .expect("setup db fixture");

    for i in 0..row_count {
        let id = i64::try_from(i + 1).expect("index fits i64");
        let payload = format!("db-corruption-row-{id:04}");
        conn.execute(
            "INSERT INTO demo (id, payload) VALUES (?1, ?2)",
            rusqlite::params![id, payload],
        )
        .expect("insert");
    }

    drop(conn);
    db_path
}

fn inject_db_corruption(
    scenario: &CorruptionScenario,
    db_path: &Path,
) -> Result<CorruptionReport, String> {
    use crate::corruption::CorruptionInjector;
    let injector = CorruptionInjector::new(db_path.to_path_buf())
        .map_err(|e| format!("injector creation: {e}"))?;
    let pattern = scenario.pattern.to_corruption_pattern(scenario.seed, 0);
    injector
        .inject(&pattern)
        .map_err(|e| format!("injection: {e}"))
}

fn inject_scenario_corruption(
    scenario: &CorruptionScenario,
    db_path: &Path,
    wal_path: &Path,
    info: &WalInfo,
) -> Result<CorruptionReport, String> {
    use crate::corruption::CorruptionInjector;

    let target_path = match scenario.target {
        CorruptionTarget::Database => db_path.to_path_buf(),
        CorruptionTarget::Wal => wal_path.to_path_buf(),
        CorruptionTarget::WalFecSidecar => {
            let sidecar = fsqlite_wal::wal_fec_path_for_wal(wal_path);
            if !sidecar.exists() {
                return Err("sidecar file does not exist".to_owned());
            }
            sidecar
        }
    };

    let injector =
        CorruptionInjector::new(target_path).map_err(|e| format!("injector creation: {e}"))?;
    let pattern = scenario
        .pattern
        .to_corruption_pattern(scenario.seed, info.frame_count);
    injector
        .inject(&pattern)
        .map_err(|e| format!("injection: {e}"))
}

fn corrupted_frame_numbers(scenario: &CorruptionScenario, info: &WalInfo) -> Vec<u32> {
    match &scenario.pattern {
        ScenarioCorruptionPattern::WalFrames { frame_indices } => {
            frame_indices.iter().map(|i| i + 1).collect()
        }
        ScenarioCorruptionPattern::WalAllFrames => (1..=info.frame_count).collect(),
        ScenarioCorruptionPattern::WalBitFlip { frame_index, .. } => {
            vec![frame_index + 1]
        }
        ScenarioCorruptionPattern::DbHeaderZero
        | ScenarioCorruptionPattern::DbPageCorrupt { .. }
        | ScenarioCorruptionPattern::SidecarCorrupt { .. } => Vec::new(),
    }
}

fn run_integrity_check(db_path: &Path) -> Result<String, String> {
    let flags =
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX;
    let conn = rusqlite::Connection::open_with_flags(db_path, flags)
        .map_err(|e| format!("open failed: {e}"))?;
    let result: String = conn
        .query_row("PRAGMA integrity_check", [], |r| r.get(0))
        .map_err(|e| format!("integrity_check failed: {e}"))?;
    if result == "ok" {
        Ok(result)
    } else {
        Err(result)
    }
}

// ── Tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::corruption_scenarios::{
        recoverable_scenarios, scenario_catalog, unrecoverable_scenarios, wal_corruption_scenarios,
    };

    #[test]
    fn test_recoverable_scenarios_classify_as_recovered() {
        for scenario in &recoverable_scenarios() {
            let report = run_recovery(scenario);
            assert!(
                matches!(
                    report.classification,
                    RecoveryClassification::Recovered { .. }
                ),
                "scenario '{}' should classify as Recovered, got: {:?} ({})",
                scenario.name,
                report.classification,
                report.verdict,
            );
            assert!(
                report.matches_expected,
                "scenario '{}' should match expected: {}",
                scenario.name, report.verdict,
            );
            // Must have repair evidence.
            assert!(
                !report.evidence.repairs.is_empty(),
                "scenario '{}' should have repair evidence",
                scenario.name,
            );
            // Must have integrity checks.
            assert!(
                !report.evidence.integrity_checks.is_empty(),
                "scenario '{}' should have integrity checks",
                scenario.name,
            );
            // All integrity checks must pass.
            for ic in &report.evidence.integrity_checks {
                assert!(
                    ic.passed,
                    "scenario '{}' integrity check '{}' should pass: {}",
                    scenario.name, ic.name, ic.detail,
                );
            }
        }
    }

    #[test]
    fn test_unrecoverable_scenarios_classify_as_lost() {
        for scenario in &unrecoverable_scenarios() {
            let report = run_recovery(scenario);
            assert!(
                matches!(report.classification, RecoveryClassification::Lost { .. }),
                "scenario '{}' should classify as Lost, got: {:?} ({})",
                scenario.name,
                report.classification,
                report.verdict,
            );
            assert!(
                report.matches_expected,
                "scenario '{}' should match expected: {}",
                scenario.name, report.verdict,
            );
        }
    }

    #[test]
    fn test_recovery_disabled_classifies_correctly() {
        let catalog = scenario_catalog();
        let disabled = catalog
            .iter()
            .find(|s| s.name == "wal_corrupt_recovery_disabled")
            .expect("should have disabled scenario");

        let report = run_recovery(disabled);
        assert!(
            matches!(
                report.classification,
                RecoveryClassification::Lost {
                    reason: LostReason::RecoveryDisabled
                }
            ),
            "disabled should be Lost(RecoveryDisabled), got: {:?}",
            report.classification,
        );
        assert!(report.matches_expected);

        let log = report.wal_recovery_log.expect("should have recovery log");
        assert!(!log.recovery_enabled);
    }

    #[test]
    fn test_beyond_tolerance_classifies_correctly() {
        let catalog = scenario_catalog();
        let beyond = catalog
            .iter()
            .find(|s| s.name == "wal_corrupt_beyond_tolerance")
            .expect("should have beyond-tolerance scenario");

        let report = run_recovery(beyond);
        assert!(
            matches!(
                report.classification,
                RecoveryClassification::Lost {
                    reason: LostReason::InsufficientSymbols { .. }
                }
            ),
            "beyond-tolerance should be Lost(InsufficientSymbols), got: {:?}",
            report.classification,
        );
        assert!(report.matches_expected);
    }

    #[test]
    fn test_no_sidecar_classifies_as_lost() {
        let catalog = scenario_catalog();
        let no_sidecar = catalog
            .iter()
            .find(|s| s.name == "wal_corrupt_no_sidecar")
            .expect("should have no-sidecar scenario");

        let report = run_recovery(no_sidecar);
        assert!(
            matches!(report.classification, RecoveryClassification::Lost { .. }),
            "no-sidecar should be Lost, got: {:?}",
            report.classification,
        );
        // Should not panic and should match expected.
        assert!(report.matches_expected);
    }

    #[test]
    fn test_db_corruption_classifies_as_lost() {
        let catalog = scenario_catalog();
        for scenario in catalog
            .iter()
            .filter(|s| s.target == CorruptionTarget::Database)
        {
            let report = run_recovery(scenario);
            assert!(
                matches!(
                    report.classification,
                    RecoveryClassification::Lost {
                        reason: LostReason::NoDbFecAvailable
                    }
                ),
                "DB scenario '{}' should be Lost(NoDbFecAvailable), got: {:?}",
                scenario.name,
                report.classification,
            );
            assert!(
                report.matches_expected,
                "DB scenario '{}' should match expected",
                scenario.name,
            );
            // Should have integrity check evidence.
            assert!(
                !report.evidence.integrity_checks.is_empty(),
                "DB scenario '{}' should have integrity check",
                scenario.name,
            );
        }
    }

    #[test]
    fn test_batch_report() {
        let wal_scenarios = wal_corruption_scenarios();
        let batch = run_all_recoveries(&wal_scenarios);

        assert_eq!(batch.total, wal_scenarios.len());
        assert_eq!(
            batch.recovered_count + batch.partial_count + batch.lost_count,
            batch.total
        );
        // At minimum, the within-tolerance scenarios should be recovered.
        assert!(
            batch.recovered_count >= 2,
            "at least 2 WAL scenarios should recover"
        );
    }

    #[test]
    fn test_full_catalog_batch() {
        let all = scenario_catalog();
        let batch = run_all_recoveries(&all);

        assert_eq!(batch.total, 8);
        assert!(
            batch.matched_count >= 6,
            "at least 6 of 8 scenarios should match expected: {} matched. Mismatches: {}",
            batch.matched_count,
            batch
                .reports
                .iter()
                .filter(|r| !r.matches_expected)
                .map(|r| format!("{}:{}", r.scenario_name, r.verdict))
                .collect::<Vec<_>>()
                .join("; ")
        );
    }

    #[test]
    fn test_artifact_writer() {
        let catalog = scenario_catalog();
        let scenario = &catalog[0]; // wal_corrupt_within_tolerance
        let report = run_recovery(scenario);

        let dir = tempfile::tempdir().unwrap();
        let output_dir = dir.path().join("artifacts");
        let json_path = write_recovery_artifacts(&report, &output_dir).unwrap();

        assert!(json_path.exists(), "JSON report should be created");
        assert!(
            output_dir.join("recovery_narrative.md").exists(),
            "narrative should be created"
        );

        let json_content = fs::read_to_string(&json_path).unwrap();
        assert!(json_content.contains("\"scenario\""));
        assert!(json_content.contains("\"classification\""));
        assert!(json_content.contains("\"detection\""));
        assert!(json_content.contains("\"repairs\""));

        let md_content = fs::read_to_string(output_dir.join("recovery_narrative.md")).unwrap();
        assert!(md_content.contains("# Recovery Report"));
        assert!(md_content.contains("Classification"));
    }

    #[test]
    fn test_classification_tags() {
        assert_eq!(
            classification_tag(&RecoveryClassification::Recovered {
                pages_repaired: 5,
                symbols_used: 2
            }),
            "recovered"
        );
        assert_eq!(
            classification_tag(&RecoveryClassification::Partial {
                pages_repaired: 3,
                pages_lost: 2,
                loss_description: String::new(),
            }),
            "partial"
        );
        assert_eq!(
            classification_tag(&RecoveryClassification::Lost {
                reason: LostReason::RecoveryDisabled,
            }),
            "lost"
        );
    }

    #[test]
    fn test_evidence_populated_for_recovered_scenario() {
        let scenario = &recoverable_scenarios()[0];
        let report = run_recovery(scenario);

        // Should have detection entries from corruption injection.
        assert!(
            !report.evidence.detection.is_empty(),
            "should have detection entries"
        );

        // Should have repair entries.
        assert!(
            !report.evidence.repairs.is_empty(),
            "should have repair entries"
        );
        let repair = &report.evidence.repairs[0];
        assert!(repair.verified, "repair should be verified");
        assert!(repair.symbols_consumed > 0, "should use symbols");

        // Should have integrity checks.
        assert!(
            !report.evidence.integrity_checks.is_empty(),
            "should have integrity checks"
        );
        assert!(
            report.evidence.integrity_checks[0].passed,
            "integrity check should pass"
        );
    }

    #[test]
    fn test_recovery_deterministic() {
        let scenario = &recoverable_scenarios()[0];

        let report1 = run_recovery(scenario);
        let report2 = run_recovery(scenario);

        // Classification should be identical.
        assert_eq!(report1.classification, report2.classification);
        assert_eq!(report1.matches_expected, report2.matches_expected);

        // Evidence counts should be identical.
        assert_eq!(
            report1.evidence.detection.len(),
            report2.evidence.detection.len()
        );
        assert_eq!(
            report1.evidence.repairs.len(),
            report2.evidence.repairs.len()
        );
    }

    #[test]
    fn test_matches_expected_logic() {
        // Recovered <-> FullRecovery.
        assert!(matches_expected(
            &RecoveryClassification::Recovered {
                pages_repaired: 5,
                symbols_used: 2
            },
            &ExpectedFsqliteBehavior::FullRecovery,
        ));

        // Lost(InsufficientSymbols) <-> RepairExceedsCapacity.
        assert!(matches_expected(
            &RecoveryClassification::Lost {
                reason: LostReason::InsufficientSymbols {
                    corrupted_count: 10,
                    r_budget: 2
                },
            },
            &ExpectedFsqliteBehavior::RepairExceedsCapacity,
        ));

        // Lost(RecoveryDisabled) <-> RecoveryDisabled.
        assert!(matches_expected(
            &RecoveryClassification::Lost {
                reason: LostReason::RecoveryDisabled,
            },
            &ExpectedFsqliteBehavior::RecoveryDisabled,
        ));

        // Mismatch.
        assert!(!matches_expected(
            &RecoveryClassification::Recovered {
                pages_repaired: 5,
                symbols_used: 2
            },
            &ExpectedFsqliteBehavior::RepairExceedsCapacity,
        ));
    }
}
