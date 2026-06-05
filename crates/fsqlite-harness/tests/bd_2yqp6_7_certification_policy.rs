//! Track G certification-policy integration tests (bd-2yqp6.7).

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use fsqlite_harness::adversarial_search::CampaignResult;
use fsqlite_harness::certification_policy::{
    CERTIFICATION_MAX_EVIDENCE_AGE_HOURS, CERTIFICATION_MIN_VERIFICATION_PCT,
    CERTIFICATION_POLICY_ID, CERTIFICATION_POLICY_SCHEMA_VERSION, CertificationRatchetBaseline,
    CertificationRatchetCandidate, REQUIRED_CERTIFICATION_LANES, canonical_certification_policy,
    evaluate_certification_ratchets,
};
use fsqlite_harness::ci_gate_matrix::{
    ArtifactEntry, ArtifactKind, ArtifactManifest, FALLBACK_TRANSPARENCY_GATE_SCHEMA_VERSION,
    FallbackTransparencyArtifactRef, FallbackTransparencyGateStatus,
    FallbackTransparencyGateSummary,
};
use fsqlite_harness::confidence_gates::{GateDecision, build_evidence_ledger, evaluate_full};
use fsqlite_harness::drift_monitor::ParityDriftMonitor;
use fsqlite_harness::parity_invariant_catalog::{
    InvariantId, ObligationStatus, ProofKind, ProofSummaryEntry, ReleaseTraceabilityReport,
    TraceabilityEntry, build_canonical_catalog,
};
use fsqlite_harness::parity_taxonomy::{FeatureId, build_canonical_universe};
use fsqlite_harness::release_certificate::{
    CERTIFICATION_TRACEABILITY_SCHEMA_VERSION, CertificateConfig, CertificateInputs,
    CertificateVerdict, build_certificate,
};
use fsqlite_harness::verification_contract_enforcement::{
    ContractEnforcementOutcome, EnforcementDisposition,
};

const BEAD_ID: &str = "bd-2yqp6.7";

fn passing_contract_outcome() -> ContractEnforcementOutcome {
    ContractEnforcementOutcome {
        schema_version: 1,
        bead_id: "bd-1dp9.7.7".to_owned(),
        base_gate_passed: true,
        contract_passed: true,
        final_gate_passed: true,
        disposition: EnforcementDisposition::Allowed,
        total_beads: 1,
        failing_beads: 0,
        missing_evidence_beads: 0,
        invalid_reference_beads: 0,
        bead_verdicts: Vec::new(),
    }
}

fn failing_contract_outcome() -> ContractEnforcementOutcome {
    ContractEnforcementOutcome {
        schema_version: 1,
        bead_id: "bd-1dp9.7.7".to_owned(),
        base_gate_passed: true,
        contract_passed: false,
        final_gate_passed: false,
        disposition: EnforcementDisposition::BlockedByContract,
        total_beads: 2,
        failing_beads: 1,
        missing_evidence_beads: 1,
        invalid_reference_beads: 0,
        bead_verdicts: Vec::new(),
    }
}

fn g9_artifact_ref(
    artifact_id: &str,
    path: &str,
    schema_version: &str,
) -> FallbackTransparencyArtifactRef {
    FallbackTransparencyArtifactRef {
        artifact_id: artifact_id.to_owned(),
        path: path.to_owned(),
        content_hash: "f".repeat(64),
        schema_version: schema_version.to_owned(),
        validation_command: "rch exec -- cargo test -p fsqlite-core --test agent_swarm_fallback_transparency_contract deterministic_fallback_denial_replay -- --nocapture".to_owned(),
        validation_passed: true,
    }
}

fn passing_g9_gate() -> FallbackTransparencyGateSummary {
    FallbackTransparencyGateSummary {
        schema_version: FALLBACK_TRANSPARENCY_GATE_SCHEMA_VERSION.to_owned(),
        status: FallbackTransparencyGateStatus::Pass,
        source_commit: "deadbeefcafebabe".to_owned(),
        generated_at: "2026-06-05T10:00:00Z".to_owned(),
        inventory: g9_artifact_ref(
            "fallback_boundary_inventory",
            "docs/contracts/fallback_boundary_inventory.toml",
            "fallback_boundary_inventory.v1",
        ),
        schema_validation: g9_artifact_ref(
            "fallback_decision_schema",
            "crates/fsqlite-core/tests/agent_swarm_fallback_transparency_contract.rs",
            "fallback_decision_schema.v1",
        ),
        replay_bundle: g9_artifact_ref(
            "fallback_denial_replay",
            "artifacts/g9/fallback_denial_replay_summary.json",
            "fallback_denial_replay.v1",
        ),
        backend_identity_summary: "fsqlite:pager_wal_mvcc_btree:parity_cert_strict".to_owned(),
        covered_boundary_ids: vec![
            "conn.select.with_clause_materialization".to_owned(),
            "conn.select.view_materialization".to_owned(),
            "vdbe.open_storage_cursor.mempage_fallback".to_owned(),
        ],
        missing_boundary_ids: Vec::new(),
        stale_artifacts: Vec::new(),
        certifying_fallback_events: 0,
        non_cert_control_events: 1,
        gate_failures: Vec::new(),
        replay_command: "rch exec -- env CARGO_TARGET_DIR=/data/tmp/frankensqlite-g9-fallback-denial-replay-target cargo test -p fsqlite-core --test agent_swarm_fallback_transparency_contract deterministic_fallback_denial_replay -- --nocapture".to_owned(),
    }
}

fn failing_g9_gate() -> FallbackTransparencyGateSummary {
    FallbackTransparencyGateSummary {
        status: FallbackTransparencyGateStatus::Fail,
        missing_boundary_ids: vec!["conn.select.sqlite_schema_virtual_materialization".to_owned()],
        stale_artifacts: vec!["fallback_denial_replay".to_owned()],
        certifying_fallback_events: 1,
        gate_failures: vec![
            "missing_boundary_coverage".to_owned(),
            "certifying_fallback_allowed".to_owned(),
            "stale_fallback_artifact".to_owned(),
        ],
        ..passing_g9_gate()
    }
}

fn certification_manifest(contract: Option<ContractEnforcementOutcome>) -> ArtifactManifest {
    ArtifactManifest {
        schema_version: "1.0.0".to_owned(),
        bead_id: BEAD_ID.to_owned(),
        run_id: "run-cert-001".to_owned(),
        lane: "e2e-differential".to_owned(),
        git_sha: "deadbeefcafebabe".to_owned(),
        seed: 42,
        created_at: "2026-04-09T12:00:00Z".to_owned(),
        artifacts: vec![ArtifactEntry {
            kind: ArtifactKind::Benchmark,
            path: "bench/scorecards.json".to_owned(),
            content_hash: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                .to_owned(),
            size_bytes: 512,
            description: "aligned causal scorecards".to_owned(),
        }],
        gate_passed: true,
        bisect_request: None,
        bisect_result_summary: None,
        verification_contract: contract,
        fallback_transparency_gate: Some(passing_g9_gate()),
    }
}

fn certification_manifest_with_g9(
    contract: Option<ContractEnforcementOutcome>,
    fallback_transparency_gate: Option<FallbackTransparencyGateSummary>,
) -> ArtifactManifest {
    ArtifactManifest {
        fallback_transparency_gate,
        ..certification_manifest(contract)
    }
}

fn certification_traceability() -> ReleaseTraceabilityReport {
    ReleaseTraceabilityReport {
        schema_version: 1,
        entries: vec![TraceabilityEntry {
            invariant_id: InvariantId("PAR-SQL-999".to_owned()),
            feature_id: FeatureId("sql.insert.autocommit".to_owned()),
            category: "Core SQL".to_owned(),
            statement: "Simple INSERT remains behaviorally identical to the oracle.".to_owned(),
            verified: true,
            proof_summary: vec![ProofSummaryEntry {
                kind: ProofKind::E2eTest,
                status: ObligationStatus::Verified,
                test_path: "fsqlite_e2e::oracle::insert_certification".to_owned(),
            }],
            artifact_refs: vec!["bench/scorecards.json".to_owned()],
        }],
        verification_pct: 100.0,
        release_ready: true,
    }
}

fn synthetic_passing_campaign() -> CampaignResult {
    CampaignResult {
        schema_version: 1,
        base_seed: 42,
        total_trials: 0,
        counterexamples_found: 0,
        by_severity: BTreeMap::new(),
        by_category: BTreeMap::new(),
        counterexamples: Vec::new(),
        passed: true,
        summary: "no adversarial findings".to_owned(),
    }
}

fn strict_ready_inputs(
    contract: ContractEnforcementOutcome,
) -> (CertificateInputs, CertificateConfig) {
    let config = CertificateConfig::default();
    let catalog = build_canonical_catalog();
    let universe = build_canonical_universe();
    let (mut gate_report, ranking) = evaluate_full(&catalog, &universe, &config.gate_config);
    gate_report.global_decision = GateDecision::Pass;
    gate_report.release_ready = true;
    gate_report.global_verification_pct = 100.0;
    gate_report.passing_invariants = gate_report.total_invariants;

    let ledger = build_evidence_ledger(&gate_report, &ranking);
    let drift_snapshot = ParityDriftMonitor::new(config.drift_config.clone()).snapshot();
    let campaign_result = synthetic_passing_campaign();

    (
        CertificateInputs {
            gate_report,
            expected_loss_ranking: ranking,
            evidence_ledger: ledger,
            catalog_stats: catalog.stats(),
            traceability: certification_traceability(),
            drift_snapshot,
            campaign_result,
            ci_flake_budget: None,
            artifact_manifest: Some(certification_manifest(Some(contract))),
        },
        config,
    )
}

#[test]
fn canonical_policy_matches_track_g_requirements() {
    let policy = canonical_certification_policy();

    assert_eq!(policy.policy_id, CERTIFICATION_POLICY_ID);
    assert_eq!(
        policy.min_verification_pct,
        CERTIFICATION_MIN_VERIFICATION_PCT
    );
    assert_eq!(
        policy.max_evidence_age_hours,
        CERTIFICATION_MAX_EVIDENCE_AGE_HOURS
    );
    assert_eq!(policy.gate_config.category_min_verification_pct, 100.0);
    assert_eq!(policy.ratchet_policy.regression_tolerance, 0.0);
    assert!(!policy.ratchet_policy.quarantine_enabled);
    assert!(!policy.ratchet_policy.waivers_enabled);
    for lane in REQUIRED_CERTIFICATION_LANES {
        assert!(
            policy
                .required_ci_lanes
                .iter()
                .any(|entry| entry == lane.as_str()),
            "bead_id={BEAD_ID} case=missing_lane lane={}",
            lane.as_str(),
        );
    }
}

#[test]
fn canonical_policy_exposes_explicit_blocking_gate_and_ratchet_dimensions() {
    let policy = canonical_certification_policy();

    let gate_ids: BTreeSet<_> = policy
        .gates
        .iter()
        .map(|gate| gate.gate_id.as_str())
        .collect();
    for required_gate in [
        "declared_surface_parity",
        "verification_contract",
        "release_evidence_completeness",
        "critical_path_evidence",
    ] {
        assert!(
            gate_ids.contains(required_gate),
            "bead_id={BEAD_ID} case=missing_gate gate={required_gate}",
        );
    }

    for lane in REQUIRED_CERTIFICATION_LANES {
        let gate_id = format!("required_suite_pass::{}", lane.as_str());
        assert!(
            gate_ids.contains(gate_id.as_str()),
            "bead_id={BEAD_ID} case=missing_required_suite_gate gate={gate_id}",
        );
    }

    assert!(
        policy.gates.iter().all(|gate| gate.blocking),
        "bead_id={BEAD_ID} case=non_blocking_gate_present",
    );

    let ratchet_ids: BTreeSet<_> = policy
        .ratchets
        .iter()
        .map(|ratchet| ratchet.ratchet_id.as_str())
        .collect();
    let expected_ratchets = BTreeSet::from([
        "global_lower_bound",
        "category_lower_bounds",
        "required_suite_pass_rate",
        "traceability_link_coverage",
        "artifact_hash_integrity",
    ]);
    assert_eq!(
        ratchet_ids, expected_ratchets,
        "bead_id={BEAD_ID} case=ratchet_dimension_drift",
    );
    assert!(
        policy.ratchets.iter().all(|ratchet| ratchet.blocking),
        "bead_id={BEAD_ID} case=non_blocking_ratchet_present",
    );
}

#[test]
fn certification_ratchet_blocks_required_suite_pass_rate_backslide() {
    let baseline = CertificationRatchetBaseline {
        schema_version: CERTIFICATION_POLICY_SCHEMA_VERSION,
        policy_id: CERTIFICATION_POLICY_ID.to_owned(),
        global_lower_bound: 1.0,
        category_lower_bounds: BTreeMap::from([
            ("Core SQL".to_owned(), 1.0),
            ("Transactions".to_owned(), 1.0),
        ]),
        required_suite_pass_rate_pct: 100.0,
        traceability_link_coverage_pct: 100.0,
    };
    let candidate = CertificationRatchetCandidate {
        global_lower_bound: 1.0,
        category_lower_bounds: BTreeMap::from([
            ("Core SQL".to_owned(), 1.0),
            ("Transactions".to_owned(), 1.0),
        ]),
        required_suite_pass_rate_pct: 83.333_333,
        traceability_link_coverage_pct: 100.0,
    };

    let evaluation = evaluate_certification_ratchets(&baseline, &candidate);
    assert!(
        !evaluation.passed,
        "bead_id={BEAD_ID} case=synthetic_suite_backslide_must_block evaluation={evaluation:?}",
    );
    assert_eq!(
        evaluation.regressed_ratchets,
        vec!["required_suite_pass_rate".to_owned()],
        "bead_id={BEAD_ID} case=expected_single_suite_regression evaluation={evaluation:?}",
    );
    assert!(
        evaluation.summary.contains("required_suite_pass_rate"),
        "bead_id={BEAD_ID} case=regression_summary_must_name_backslide summary={}",
        evaluation.summary,
    );
}

#[test]
fn release_certificate_embeds_feature_test_run_artifact_chain() {
    let (inputs, config) = strict_ready_inputs(passing_contract_outcome());
    let cert = build_certificate(&inputs, &config);

    assert_eq!(cert.certification_policy_id, CERTIFICATION_POLICY_ID);
    assert_eq!(
        cert.certification_traceability.schema_version,
        CERTIFICATION_TRACEABILITY_SCHEMA_VERSION,
    );
    assert!(
        cert.certification_evidence.artifact_manifest_present,
        "bead_id={BEAD_ID} case=manifest_present",
    );
    assert_eq!(
        cert.certification_traceability.fully_linked_entries, 1,
        "bead_id={BEAD_ID} case=linked_entries",
    );
    assert_eq!(
        cert.certification_evidence
            .fallback_transparency_gate_passed,
        Some(true),
        "bead_id={BEAD_ID} case=g9_gate_passed",
    );

    let entry = &cert.certification_traceability.entries[0];
    assert_eq!(
        entry.proof_summary[0].test_path,
        "fsqlite_e2e::oracle::insert_certification",
    );
    assert_eq!(
        entry.run.as_ref().map(|run| run.run_id.as_str()),
        Some("run-cert-001")
    );
    assert_eq!(entry.artifacts.len(), 1);
    assert_eq!(
        entry.artifacts[0].content_hash,
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    );
}

#[test]
fn release_certificate_rejects_manifest_missing_g9_fallback_gate() {
    let (mut inputs, config) = strict_ready_inputs(passing_contract_outcome());
    inputs.artifact_manifest = Some(certification_manifest_with_g9(
        Some(passing_contract_outcome()),
        None,
    ));

    let cert = build_certificate(&inputs, &config);
    assert_eq!(cert.verdict, CertificateVerdict::Rejected);
    assert!(
        cert.unresolved_risks
            .iter()
            .any(|risk| risk.source == "fallback_transparency_gate"
                && risk.description.contains("evidence is missing")),
        "bead_id={BEAD_ID} case=missing_g9_gate_risk",
    );
}

#[test]
fn release_certificate_rejects_failed_g9_fallback_gate() {
    let (mut inputs, config) = strict_ready_inputs(passing_contract_outcome());
    inputs.artifact_manifest = Some(certification_manifest_with_g9(
        Some(passing_contract_outcome()),
        Some(failing_g9_gate()),
    ));

    let cert = build_certificate(&inputs, &config);
    assert_eq!(cert.verdict, CertificateVerdict::Rejected);
    assert_eq!(
        cert.certification_evidence
            .fallback_transparency_certifying_fallback_event_count,
        1,
        "bead_id={BEAD_ID} case=g9_certifying_fallback_count",
    );
    assert!(
        cert.unresolved_risks
            .iter()
            .any(|risk| risk.source == "fallback_transparency_gate"
                && risk
                    .description
                    .contains("G9 fallback-transparency gate failed")),
        "bead_id={BEAD_ID} case=failed_g9_gate_risk",
    );
}

#[test]
fn release_certificate_rejects_failed_verification_contract_from_manifest() {
    let (inputs, config) = strict_ready_inputs(failing_contract_outcome());
    let cert = build_certificate(&inputs, &config);

    assert_eq!(cert.verdict, CertificateVerdict::Rejected);
    assert_eq!(cert.certification_evidence.missing_evidence_beads, 1);
    assert!(
        cert.unresolved_risks
            .iter()
            .any(|risk| risk.source == "verification_contract"),
        "bead_id={BEAD_ID} case=contract_risk",
    );
}

#[test]
fn release_certificate_rejects_manifest_missing_verification_contract() {
    let config = CertificateConfig::default();
    let catalog = build_canonical_catalog();
    let universe = build_canonical_universe();
    let (mut gate_report, ranking) = evaluate_full(&catalog, &universe, &config.gate_config);
    gate_report.global_decision = GateDecision::Pass;
    gate_report.release_ready = true;
    gate_report.global_verification_pct = 100.0;
    gate_report.passing_invariants = gate_report.total_invariants;

    let inputs = CertificateInputs {
        gate_report: gate_report.clone(),
        expected_loss_ranking: ranking.clone(),
        evidence_ledger: build_evidence_ledger(&gate_report, &ranking),
        catalog_stats: catalog.stats(),
        traceability: certification_traceability(),
        drift_snapshot: ParityDriftMonitor::new(config.drift_config.clone()).snapshot(),
        campaign_result: synthetic_passing_campaign(),
        ci_flake_budget: None,
        artifact_manifest: Some(certification_manifest(None)),
    };

    let cert = build_certificate(&inputs, &config);
    assert_eq!(cert.verdict, CertificateVerdict::Rejected);
    assert!(
        cert.unresolved_risks.iter().any(|risk| risk
            .description
            .contains("verification-contract evidence is missing")),
        "bead_id={BEAD_ID} case=missing_contract_evidence",
    );
}
