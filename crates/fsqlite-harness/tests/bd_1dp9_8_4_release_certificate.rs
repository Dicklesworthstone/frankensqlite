//! Integration tests for bd-1dp9.8.4 — Release Certificate Generator.
//!
//! Tests the full release certificate pipeline that aggregates parity
//! invariant catalog, drift monitors, confidence gates, adversarial search,
//! and CI artifacts into a machine-verifiable release certificate.

use fsqlite_harness::adversarial_search::{AdversarialConfig, run_campaign};
use fsqlite_harness::ci_gate_matrix::ArtifactEntry;
use fsqlite_harness::confidence_gates::{
    GateConfig, GateDecision, build_evidence_ledger, evaluate_full,
};
use fsqlite_harness::drift_monitor::{ParityDriftConfig, ParityDriftMonitor};
use fsqlite_harness::parity_invariant_catalog::build_canonical_catalog;
use fsqlite_harness::parity_taxonomy::build_canonical_universe;
use fsqlite_harness::release_certificate::{
    CERTIFICATE_SCHEMA_VERSION, CertificateConfig, CertificateInputs, CertificateVerdict,
    RELEASE_CERT_BEAD_ID, ReleaseCertificate, build_certificate, generate_release_certificate,
    load_certificate, write_certificate,
};

const BEAD_ID: &str = "bd-1dp9.8.4";

// ---------------------------------------------------------------------------
// Full pipeline
// ---------------------------------------------------------------------------

#[test]
fn full_pipeline_produces_valid_certificate() {
    let config = CertificateConfig::default();
    let cert = generate_release_certificate(&config);

    assert_eq!(
        cert.bead_id, RELEASE_CERT_BEAD_ID,
        "bead_id={BEAD_ID} case=bead_id"
    );
    assert_eq!(
        cert.schema_version, CERTIFICATE_SCHEMA_VERSION,
        "bead_id={BEAD_ID} case=schema_version"
    );
}

#[test]
fn full_pipeline_has_all_evidence_chain_entries() {
    let config = CertificateConfig::default();
    let cert = generate_release_certificate(&config);

    // Should reference all 4 source beads: 8.1, 8.2, 8.3, 8.5
    let sources: Vec<&str> = cert.evidence_chain.iter().map(|e| e.source_bead.as_str()).collect();
    assert!(
        sources.contains(&"bd-1dp9.8.1"),
        "bead_id={BEAD_ID} case=chain_8_1"
    );
    assert!(
        sources.contains(&"bd-1dp9.8.2"),
        "bead_id={BEAD_ID} case=chain_8_2"
    );
    assert!(
        sources.contains(&"bd-1dp9.8.3"),
        "bead_id={BEAD_ID} case=chain_8_3"
    );
    assert!(
        sources.contains(&"bd-1dp9.8.5"),
        "bead_id={BEAD_ID} case=chain_8_5"
    );
}

#[test]
fn full_pipeline_has_score_bounds() {
    let config = CertificateConfig::default();
    let cert = generate_release_certificate(&config);

    assert!(
        cert.global_posterior_mean >= 0.0 && cert.global_posterior_mean <= 1.0,
        "bead_id={BEAD_ID} case=posterior_bounds mean={}",
        cert.global_posterior_mean,
    );
    assert!(
        cert.global_lower_bound <= cert.global_posterior_mean,
        "bead_id={BEAD_ID} case=lower_bound_ordering",
    );
    assert!(
        cert.global_verification_pct >= 0.0,
        "bead_id={BEAD_ID} case=verification_pct",
    );
}

#[test]
fn full_pipeline_has_invariant_counts() {
    let config = CertificateConfig::default();
    let cert = generate_release_certificate(&config);

    assert!(
        cert.total_invariants > 0,
        "bead_id={BEAD_ID} case=total_invariants"
    );
    assert!(
        cert.passing_invariants <= cert.total_invariants,
        "bead_id={BEAD_ID} case=passing_bounded"
    );
}

#[test]
fn full_pipeline_tracks_adversarial() {
    let config = CertificateConfig::default();
    let cert = generate_release_certificate(&config);

    assert!(
        cert.high_severity_count <= cert.counterexample_count,
        "bead_id={BEAD_ID} case=severity_bounded"
    );
}

// ---------------------------------------------------------------------------
// Verdict logic
// ---------------------------------------------------------------------------

#[test]
fn verdict_display_values() {
    assert_eq!(CertificateVerdict::Approved.to_string(), "APPROVED");
    assert_eq!(CertificateVerdict::Conditional.to_string(), "CONDITIONAL");
    assert_eq!(CertificateVerdict::Rejected.to_string(), "REJECTED");
}

#[test]
fn verdict_rejected_when_gate_fails() {
    let config = CertificateConfig::default();
    let catalog = build_canonical_catalog();
    let universe = build_canonical_universe();
    let (mut gate_report, ranking) = evaluate_full(&catalog, &universe, &config.gate_config);
    let ledger = build_evidence_ledger(&gate_report, &ranking);

    gate_report.global_decision = GateDecision::Fail;
    gate_report.release_ready = false;

    let drift_monitor = ParityDriftMonitor::new(config.drift_config.clone());
    let drift_snapshot = drift_monitor.snapshot();
    let campaign_result = run_campaign(&config.adversarial_config);

    let inputs = CertificateInputs {
        gate_report,
        expected_loss_ranking: ranking,
        evidence_ledger: ledger,
        catalog_stats: catalog.stats(),
        traceability: catalog.release_traceability(),
        drift_snapshot,
        campaign_result,
        ci_flake_budget: None,
        artifact_hashes: Vec::new(),
    };

    let cert = build_certificate(&inputs, &config);
    assert_eq!(
        cert.verdict,
        CertificateVerdict::Rejected,
        "bead_id={BEAD_ID} case=gate_fail_rejected"
    );
}

#[test]
fn certificate_includes_unresolved_risks_on_gate_failure() {
    let config = CertificateConfig::default();
    let catalog = build_canonical_catalog();
    let universe = build_canonical_universe();
    let (mut gate_report, ranking) = evaluate_full(&catalog, &universe, &config.gate_config);
    let ledger = build_evidence_ledger(&gate_report, &ranking);

    gate_report.global_decision = GateDecision::Fail;
    gate_report.release_ready = false;

    let drift_monitor = ParityDriftMonitor::new(config.drift_config.clone());
    let inputs = CertificateInputs {
        gate_report,
        expected_loss_ranking: ranking,
        evidence_ledger: ledger,
        catalog_stats: catalog.stats(),
        traceability: catalog.release_traceability(),
        drift_snapshot: drift_monitor.snapshot(),
        campaign_result: run_campaign(&config.adversarial_config),
        ci_flake_budget: None,
        artifact_hashes: Vec::new(),
    };

    let cert = build_certificate(&inputs, &config);
    assert!(
        cert.unresolved_risks
            .iter()
            .any(|r| r.source == "confidence_gates"),
        "bead_id={BEAD_ID} case=gate_risk_present"
    );
}

// ---------------------------------------------------------------------------
// Serialization
// ---------------------------------------------------------------------------

#[test]
fn certificate_json_roundtrip() {
    let config = CertificateConfig::default();
    let cert = generate_release_certificate(&config);

    let json = cert.to_json().expect("serialize");
    let parsed = ReleaseCertificate::from_json(&json).expect("parse");

    assert_eq!(parsed.bead_id, cert.bead_id);
    assert_eq!(parsed.verdict, cert.verdict);
    assert_eq!(parsed.total_invariants, cert.total_invariants);
    assert_eq!(parsed.passing_invariants, cert.passing_invariants);
    assert_eq!(parsed.evidence_chain.len(), cert.evidence_chain.len());
}

#[test]
fn certificate_file_roundtrip() {
    let config = CertificateConfig::default();
    let cert = generate_release_certificate(&config);

    let dir = std::env::temp_dir().join("fsqlite-release-cert-integ-test");
    std::fs::create_dir_all(&dir).expect("create temp dir");
    let path = dir.join("cert-integ-test.json");

    write_certificate(&path, &cert).expect("write");
    let loaded = load_certificate(&path).expect("load");

    assert_eq!(loaded.verdict, cert.verdict);
    assert_eq!(loaded.total_invariants, cert.total_invariants);
    assert_eq!(loaded.bead_id, cert.bead_id);

    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_dir(&dir);
}

// ---------------------------------------------------------------------------
// Triage line and summary
// ---------------------------------------------------------------------------

#[test]
fn triage_line_has_key_fields() {
    let config = CertificateConfig::default();
    let cert = generate_release_certificate(&config);
    let line = cert.triage_line();

    assert!(
        line.contains("gate="),
        "bead_id={BEAD_ID} case=triage_gate"
    );
    assert!(
        line.contains("verified="),
        "bead_id={BEAD_ID} case=triage_verified"
    );
    assert!(
        line.contains("invariants="),
        "bead_id={BEAD_ID} case=triage_invariants"
    );
    assert!(
        line.contains("drift="),
        "bead_id={BEAD_ID} case=triage_drift"
    );
    assert!(
        line.contains("adversarial="),
        "bead_id={BEAD_ID} case=triage_adversarial"
    );
    assert!(
        line.contains("risks="),
        "bead_id={BEAD_ID} case=triage_risks"
    );
}

#[test]
fn summary_is_nonempty_and_informative() {
    let config = CertificateConfig::default();
    let cert = generate_release_certificate(&config);

    assert!(
        !cert.summary.is_empty(),
        "bead_id={BEAD_ID} case=summary_nonempty"
    );
    assert!(
        cert.summary.contains("Release certificate"),
        "bead_id={BEAD_ID} case=summary_header"
    );
}

// ---------------------------------------------------------------------------
// Determinism
// ---------------------------------------------------------------------------

#[test]
fn full_pipeline_is_deterministic() {
    let config = CertificateConfig::default();
    let c1 = generate_release_certificate(&config);
    let c2 = generate_release_certificate(&config);

    assert_eq!(
        c1.verdict, c2.verdict,
        "bead_id={BEAD_ID} case=det_verdict"
    );
    assert_eq!(
        c1.total_invariants, c2.total_invariants,
        "bead_id={BEAD_ID} case=det_invariants"
    );
    assert_eq!(
        c1.passing_invariants, c2.passing_invariants,
        "bead_id={BEAD_ID} case=det_passing"
    );
    assert_eq!(
        c1.to_json().unwrap(),
        c2.to_json().unwrap(),
        "bead_id={BEAD_ID} case=det_json"
    );
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

#[test]
fn config_default_is_reasonable() {
    let config = CertificateConfig::default();
    assert!(
        config.min_verification_pct > 0.0 && config.min_verification_pct <= 1.0,
        "bead_id={BEAD_ID} case=min_pct"
    );
    assert_eq!(
        config.max_high_severity, 0,
        "bead_id={BEAD_ID} case=max_high_zero"
    );
}

// ---------------------------------------------------------------------------
// Embedded ledger consistency
// ---------------------------------------------------------------------------

#[test]
fn embedded_ledger_matches_gate_decision() {
    let config = CertificateConfig::default();
    let cert = generate_release_certificate(&config);

    assert_eq!(
        cert.evidence_ledger.global_decision, cert.gate_decision,
        "bead_id={BEAD_ID} case=ledger_gate_match"
    );
    assert_eq!(
        cert.evidence_ledger.release_ready, cert.gate_release_ready,
        "bead_id={BEAD_ID} case=ledger_release_match"
    );
}
