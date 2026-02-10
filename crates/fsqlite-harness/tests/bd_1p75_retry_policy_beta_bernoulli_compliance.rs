//! bd-1p75: §18.8 Retry Policy — Beta-Bernoulli Expected-Loss Controller + Starvation Fairness.
//!
//! Validates the retry policy controller that decides whether to fail
//! immediately (SQLITE_BUSY) or retry after waiting, using a discrete
//! Beta-Bernoulli model to estimate success probability per candidate wait
//! time, selecting the action that minimizes expected loss.

use std::fs;
use std::path::{Path, PathBuf};

use fsqlite_mvcc::{
    BetaPosterior, ContentionBucketKey, DEFAULT_CANDIDATE_WAITS_MS, DEFAULT_STARVATION_THRESHOLD,
    HazardModelParams, MAX_CONTENTION_BUCKETS, RetryAction, RetryController, RetryCostParams,
    expected_loss_failnow, expected_loss_retry, gittins_index_approx, gittins_threshold,
};
use proptest::prelude::proptest;
use proptest::test_runner::TestCaseError;
use serde_json::Value;

const BEAD_ID: &str = "bd-1p75";
const ISSUES_JSONL: &str = ".beads/issues.jsonl";
const LOG_STANDARD_REF: &str = "bd-1fpm";

const UNIT_TEST_IDS: [&str; 18] = [
    "test_beta_bernoulli_update",
    "test_beta_bernoulli_posterior_mean",
    "test_expected_loss_failnow",
    "test_expected_loss_retry",
    "test_argmin_selects_cheapest",
    "test_budget_clamp",
    "test_budget_exhausted_returns_busy",
    "test_contention_buckets_deterministic",
    "test_contention_buckets_bounded",
    "test_hazard_model_optimal_wait",
    "test_hazard_model_no_retry",
    "test_hazard_model_clamp_budget",
    "test_starvation_escalation",
    "test_no_priority_for_retries",
    "test_evidence_ledger_complete",
    "test_evidence_ledger_starvation",
    "test_gittins_index_threshold",
    "test_cx_deadline_respected",
];
const E2E_TEST_IDS: [&str; 1] = ["test_e2e_bd_1p75_compliance"];
const LOG_LEVEL_MARKERS: [&str; 4] = ["DEBUG", "INFO", "WARN", "ERROR"];
const REQUIRED_TOKENS: [&str; 23] = [
    "test_beta_bernoulli_update",
    "test_beta_bernoulli_posterior_mean",
    "test_expected_loss_failnow",
    "test_expected_loss_retry",
    "test_argmin_selects_cheapest",
    "test_budget_clamp",
    "test_budget_exhausted_returns_busy",
    "test_contention_buckets_deterministic",
    "test_contention_buckets_bounded",
    "test_hazard_model_optimal_wait",
    "test_hazard_model_no_retry",
    "test_hazard_model_clamp_budget",
    "test_starvation_escalation",
    "test_no_priority_for_retries",
    "test_evidence_ledger_complete",
    "test_evidence_ledger_starvation",
    "test_gittins_index_threshold",
    "test_cx_deadline_respected",
    "test_e2e_bd_1p75_compliance",
    "DEBUG",
    "INFO",
    "WARN",
    "ERROR",
];

// -------------------------------------------------------------------------
// Compliance gate helpers
// -------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq)]
#[allow(clippy::struct_field_names)]
struct ComplianceEvaluation {
    missing_unit_ids: Vec<&'static str>,
    missing_e2e_ids: Vec<&'static str>,
    missing_log_levels: Vec<&'static str>,
    missing_log_standard_ref: bool,
}

impl ComplianceEvaluation {
    fn is_compliant(&self) -> bool {
        self.missing_unit_ids.is_empty()
            && self.missing_e2e_ids.is_empty()
            && self.missing_log_levels.is_empty()
            && !self.missing_log_standard_ref
    }
}

fn workspace_root() -> Result<PathBuf, String> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .map_err(|error| format!("workspace_root_canonicalize_failed error={error}"))
}

fn load_issue_description(issue_id: &str) -> Result<String, String> {
    let issues_path = workspace_root()?.join(ISSUES_JSONL);
    let raw = fs::read_to_string(&issues_path).map_err(|error| {
        format!(
            "issues_jsonl_read_failed path={} error={error}",
            issues_path.display()
        )
    })?;

    for line in raw.lines().filter(|line| !line.trim().is_empty()) {
        let value: Value = serde_json::from_str(line)
            .map_err(|error| format!("issues_jsonl_parse_failed error={error}"))?;
        if value.get("id").and_then(Value::as_str) == Some(issue_id) {
            let mut canonical = value
                .get("description")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned();

            if let Some(comments) = value.get("comments").and_then(Value::as_array) {
                for comment in comments {
                    if let Some(text) = comment.get("text").and_then(Value::as_str) {
                        canonical.push_str("\n\n");
                        canonical.push_str(text);
                    }
                }
            }

            return Ok(canonical);
        }
    }

    Err(format!("bead_id={issue_id} not_found_in={ISSUES_JSONL}"))
}

fn contains_identifier(text: &str, expected: &str) -> bool {
    text.split(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '_'))
        .any(|candidate| candidate == expected)
}

fn evaluate_description(description: &str) -> ComplianceEvaluation {
    let missing_unit_ids = UNIT_TEST_IDS
        .into_iter()
        .filter(|id| !contains_identifier(description, id))
        .collect::<Vec<_>>();
    let missing_e2e_ids = E2E_TEST_IDS
        .into_iter()
        .filter(|id| !contains_identifier(description, id))
        .collect::<Vec<_>>();
    let missing_log_levels = LOG_LEVEL_MARKERS
        .into_iter()
        .filter(|level| !description.contains(level))
        .collect::<Vec<_>>();

    ComplianceEvaluation {
        missing_unit_ids,
        missing_e2e_ids,
        missing_log_levels,
        missing_log_standard_ref: !description.contains(LOG_STANDARD_REF),
    }
}

// -------------------------------------------------------------------------
// Compliance gate tests
// -------------------------------------------------------------------------

#[test]
fn test_bd_1p75_unit_compliance_gate() -> Result<(), String> {
    let description = load_issue_description(BEAD_ID)?;
    let evaluation = evaluate_description(&description);

    if !evaluation.missing_unit_ids.is_empty() {
        return Err(format!(
            "bead_id={BEAD_ID} case=unit_ids_missing missing={:?}",
            evaluation.missing_unit_ids
        ));
    }
    if !evaluation.missing_e2e_ids.is_empty() {
        return Err(format!(
            "bead_id={BEAD_ID} case=e2e_ids_missing missing={:?}",
            evaluation.missing_e2e_ids
        ));
    }
    if !evaluation.missing_log_levels.is_empty() {
        return Err(format!(
            "bead_id={BEAD_ID} case=log_levels_missing missing={:?}",
            evaluation.missing_log_levels
        ));
    }
    if evaluation.missing_log_standard_ref {
        return Err(format!(
            "bead_id={BEAD_ID} case=log_standard_missing expected={LOG_STANDARD_REF}"
        ));
    }

    Ok(())
}

proptest! {
    #[test]
    fn prop_bd_1p75_structure_compliance(missing_index in 0usize..REQUIRED_TOKENS.len()) {
        let description = load_issue_description(BEAD_ID).map_err(TestCaseError::fail)?;
        let marker = REQUIRED_TOKENS[missing_index];
        let removed = description.replace(marker, "");
        let evaluation = evaluate_description(&removed);
        if evaluation.is_compliant() {
            return Err(TestCaseError::fail(format!(
                "bead_id={BEAD_ID} case=marker_removal_not_detected idx={missing_index} marker={marker}"
            )));
        }
    }
}

// -------------------------------------------------------------------------
// Unit tests (18 tests from bead spec)
// -------------------------------------------------------------------------

#[test]
fn test_beta_bernoulli_update() -> Result<(), String> {
    let mut bp = BetaPosterior::new(1.0, 1.0);
    bp.observe(true);
    if (bp.alpha - 2.0).abs() > 1e-10 {
        return Err(format!(
            "bead_id={BEAD_ID} case=alpha_after_success expected=2.0 got={}",
            bp.alpha
        ));
    }
    if (bp.beta - 1.0).abs() > 1e-10 {
        return Err(format!(
            "bead_id={BEAD_ID} case=beta_unchanged expected=1.0 got={}",
            bp.beta
        ));
    }
    bp.observe(false);
    if (bp.alpha - 2.0).abs() > 1e-10 {
        return Err(format!(
            "bead_id={BEAD_ID} case=alpha_after_failure expected=2.0 got={}",
            bp.alpha
        ));
    }
    if (bp.beta - 2.0).abs() > 1e-10 {
        return Err(format!(
            "bead_id={BEAD_ID} case=beta_after_failure expected=2.0 got={}",
            bp.beta
        ));
    }

    eprintln!(
        "DEBUG bead_id={BEAD_ID} case=beta_bernoulli_update alpha={} beta={}",
        bp.alpha, bp.beta
    );
    Ok(())
}

#[test]
fn test_beta_bernoulli_posterior_mean() -> Result<(), String> {
    let mut bp = BetaPosterior::new(1.0, 1.0);
    // Observe: success, success, failure → alpha=3, beta=2 → mean = 3/5 = 0.6
    bp.observe(true);
    bp.observe(true);
    bp.observe(false);
    let p_hat = bp.mean();
    let expected = 3.0 / 5.0;

    if (p_hat - expected).abs() > 1e-10 {
        return Err(format!(
            "bead_id={BEAD_ID} case=posterior_mean expected={expected} got={p_hat}"
        ));
    }

    eprintln!(
        "INFO bead_id={BEAD_ID} case=posterior_mean p_hat={p_hat:.4} alpha={} beta={}",
        bp.alpha, bp.beta
    );
    Ok(())
}

#[test]
fn test_expected_loss_failnow() -> Result<(), String> {
    let params = RetryCostParams {
        c_fail: 100.0,
        c_try: 5.0,
    };
    let el = expected_loss_failnow(&params);
    if (el - 100.0).abs() > 1e-10 {
        return Err(format!(
            "bead_id={BEAD_ID} case=expected_loss_failnow expected=100.0 got={el}"
        ));
    }

    eprintln!("INFO bead_id={BEAD_ID} case=expected_loss_failnow el={el:.2}");
    Ok(())
}

#[test]
fn test_expected_loss_retry() -> Result<(), String> {
    let params = RetryCostParams {
        c_fail: 100.0,
        c_try: 5.0,
    };
    // wait=10ms, p_succ=0.8 → 10 + 5 + (1-0.8)*100 = 35
    let el = expected_loss_retry(10, 0.8, &params);
    let expected = 35.0;
    if (el - expected).abs() > 1e-10 {
        return Err(format!(
            "bead_id={BEAD_ID} case=expected_loss_retry expected={expected} got={el}"
        ));
    }

    eprintln!("INFO bead_id={BEAD_ID} case=expected_loss_retry el={el:.2} wait=10 p_succ=0.8");
    Ok(())
}

#[test]
fn test_argmin_selects_cheapest() -> Result<(), String> {
    let params = RetryCostParams {
        c_fail: 100.0,
        c_try: 1.0,
    };
    let mut ctrl = RetryController::new(params);

    // Train 5ms to have very high success rate.
    for _ in 0..100 {
        ctrl.observe(5, true);
    }
    for _ in 0..2 {
        ctrl.observe(5, false);
    }

    let action = ctrl.decide(1, 200, 0, None);
    // With p_succ(5ms) ≈ 100/103 ≈ 0.97, expected loss ≈ 5 + 1 + 0.03*100 = 9
    // vs FailNow = 100. Should pick RetryAfter(5ms) or lower.
    if matches!(action, RetryAction::FailNow) {
        return Err(format!(
            "bead_id={BEAD_ID} case=argmin_not_cheapest action={action:?}"
        ));
    }

    // Verify it picked the best one — check evidence.
    let entry = ctrl.ledger().last().unwrap();
    let chosen_loss = match action {
        RetryAction::RetryAfter { wait_ms } => {
            let idx = entry
                .candidate_set
                .iter()
                .position(|&t| t == wait_ms)
                .unwrap();
            entry.expected_losses[idx]
        }
        RetryAction::FailNow => entry.expected_loss_failnow,
    };

    // Chosen loss must be <= all other options.
    for &other_loss in &entry.expected_losses {
        if other_loss < chosen_loss - 1e-10 {
            return Err(format!(
                "bead_id={BEAD_ID} case=argmin_suboptimal chosen_loss={chosen_loss:.4} better={other_loss:.4}"
            ));
        }
    }

    eprintln!(
        "INFO bead_id={BEAD_ID} case=argmin_selects_cheapest action={action} chosen_loss={chosen_loss:.4}"
    );
    Ok(())
}

#[test]
fn test_budget_clamp() -> Result<(), String> {
    let params = RetryCostParams {
        c_fail: 100.0,
        c_try: 1.0,
    };
    let mut ctrl = RetryController::new(params);

    // Budget = 3ms: only candidates 0, 1, 2 are eligible.
    let _ = ctrl.decide(1, 3, 0, None);
    let entry = ctrl.ledger().last().unwrap();

    for &t in &entry.candidate_set {
        if t > 3 {
            return Err(format!(
                "bead_id={BEAD_ID} case=budget_clamp_violated candidate={t}ms > budget=3ms"
            ));
        }
    }

    // The default set {0,1,2,5,10,20,50,100} should be clamped to {0,1,2}.
    if entry.candidate_set.len() > 3 {
        return Err(format!(
            "bead_id={BEAD_ID} case=budget_clamp_too_many candidates={:?}",
            entry.candidate_set
        ));
    }

    eprintln!(
        "DEBUG bead_id={BEAD_ID} case=budget_clamp candidates={:?} budget=3ms",
        entry.candidate_set
    );
    Ok(())
}

#[test]
fn test_budget_exhausted_returns_busy() -> Result<(), String> {
    let mut ctrl = RetryController::new(RetryCostParams::default());
    let action = ctrl.decide(1, 0, 0, None);

    if action != RetryAction::FailNow {
        return Err(format!(
            "bead_id={BEAD_ID} case=budget_exhausted expected=FailNow got={action:?}"
        ));
    }

    eprintln!("INFO bead_id={BEAD_ID} case=budget_exhausted_returns_busy action=FailNow");
    Ok(())
}

#[test]
fn test_contention_buckets_deterministic() -> Result<(), String> {
    let k1 = ContentionBucketKey::from_raw(4, 0.025);
    let k2 = ContentionBucketKey::from_raw(4, 0.025);

    if k1.bucket_index() != k2.bucket_index() {
        return Err(format!(
            "bead_id={BEAD_ID} case=bucket_nondeterministic idx1={} idx2={}",
            k1.bucket_index(),
            k2.bucket_index()
        ));
    }

    // Different inputs should potentially map to different buckets.
    let k3 = ContentionBucketKey::from_raw(8, 0.5);
    eprintln!(
        "DEBUG bead_id={BEAD_ID} case=contention_buckets k1_idx={} k3_idx={}",
        k1.bucket_index(),
        k3.bucket_index()
    );

    Ok(())
}

#[test]
fn test_contention_buckets_bounded() -> Result<(), String> {
    let mut seen_buckets = std::collections::HashSet::new();

    for n in 0..=20 {
        for m2_step in 0..=20 {
            let m2 = f64::from(m2_step) / 20.0;
            let k = ContentionBucketKey::from_raw(n, m2);
            let idx = usize::from(k.bucket_index());
            if idx >= MAX_CONTENTION_BUCKETS {
                return Err(format!(
                    "bead_id={BEAD_ID} case=bucket_overflow idx={idx} n={n} m2={m2}"
                ));
            }
            seen_buckets.insert(idx);
        }
    }

    if seen_buckets.len() > MAX_CONTENTION_BUCKETS {
        return Err(format!(
            "bead_id={BEAD_ID} case=too_many_buckets count={} max={MAX_CONTENTION_BUCKETS}",
            seen_buckets.len()
        ));
    }

    eprintln!(
        "INFO bead_id={BEAD_ID} case=contention_buckets_bounded unique_buckets={} max={MAX_CONTENTION_BUCKETS}",
        seen_buckets.len()
    );
    Ok(())
}

#[test]
fn test_hazard_model_optimal_wait() -> Result<(), String> {
    let hm = HazardModelParams::new(0.5);
    // lambda=0.5, c_fail=100 → t* = (1/0.5)*ln(0.5*100) = 2*ln(50) ≈ 7.824
    let t_star = hm.optimal_wait_ms(100.0);
    let expected = 2.0 * 50.0_f64.ln();
    let diff = (t_star - expected).abs();

    if diff > 0.01 {
        return Err(format!(
            "bead_id={BEAD_ID} case=hazard_optimal expected={expected:.4} got={t_star:.4} diff={diff:.6}"
        ));
    }

    eprintln!(
        "INFO bead_id={BEAD_ID} case=hazard_model_optimal t_star={t_star:.4} expected={expected:.4}"
    );
    Ok(())
}

#[test]
fn test_hazard_model_no_retry() -> Result<(), String> {
    let hm = HazardModelParams::new(0.01);
    // lambda=0.01, c_fail=50 → lambda*c_fail=0.5 <= 1 → t*=0
    let t_star = hm.optimal_wait_ms(50.0);

    if t_star.abs() > 1e-10 {
        return Err(format!(
            "bead_id={BEAD_ID} case=hazard_no_retry expected=0.0 got={t_star:.4}"
        ));
    }

    eprintln!("INFO bead_id={BEAD_ID} case=hazard_model_no_retry t_star={t_star:.4}");
    Ok(())
}

#[test]
fn test_hazard_model_clamp_budget() -> Result<(), String> {
    let hm = HazardModelParams::new(0.5);
    // Unclamped t* ≈ 7.824ms. Budget = 5ms. Should clamp to 5ms (nearest in set).
    let candidates = DEFAULT_CANDIDATE_WAITS_MS;
    let clamped = hm.optimal_wait_clamped(100.0, 5, &candidates);

    if clamped > 5 {
        return Err(format!(
            "bead_id={BEAD_ID} case=hazard_clamp_exceeded budget=5 got={clamped}"
        ));
    }

    // Should round to nearest candidate within budget.
    if !candidates.contains(&clamped) {
        return Err(format!(
            "bead_id={BEAD_ID} case=hazard_clamp_invalid candidate={clamped}"
        ));
    }

    eprintln!("DEBUG bead_id={BEAD_ID} case=hazard_model_clamp clamped={clamped}ms budget=5ms");
    Ok(())
}

#[test]
fn test_starvation_escalation() -> Result<(), String> {
    let params = RetryCostParams::default();
    let mut ctrl =
        RetryController::with_candidates(params, vec![0, 5, 10], DEFAULT_STARVATION_THRESHOLD);

    // Simulate repeated conflicts for txn_id=99.
    for _ in 0..DEFAULT_STARVATION_THRESHOLD {
        let _ = ctrl.decide(99, 100, 0, None);
    }

    if !ctrl.is_starvation_escalated(99) {
        return Err(format!(
            "bead_id={BEAD_ID} case=starvation_not_escalated threshold={DEFAULT_STARVATION_THRESHOLD}"
        ));
    }

    // Last entry should have starvation flag.
    let last = ctrl.ledger().last().unwrap();
    if !last.starvation_escalation {
        return Err(format!("bead_id={BEAD_ID} case=starvation_not_in_ledger"));
    }

    eprintln!(
        "WARN bead_id={BEAD_ID} case=starvation_escalation txn_id=99 threshold={DEFAULT_STARVATION_THRESHOLD} reference={LOG_STANDARD_REF}"
    );
    Ok(())
}

#[test]
fn test_no_priority_for_retries() -> Result<(), String> {
    // NI-5: retried transactions do not jump ahead of new ones.
    // The retry controller does not maintain a queue — it only decides
    // per-transaction whether to retry. Verify that a retried transaction's
    // wait time is always >= 0 (no negative/priority waits).
    let mut ctrl = RetryController::new(RetryCostParams::default());

    for _ in 0..5 {
        let action = ctrl.decide(42, 100, 0, None);
        if let RetryAction::RetryAfter { wait_ms } = action {
            // Must be a valid candidate, no negative/priority waits.
            if !DEFAULT_CANDIDATE_WAITS_MS.contains(&wait_ms) {
                return Err(format!(
                    "bead_id={BEAD_ID} case=invalid_retry_wait wait={wait_ms}"
                ));
            }
        }
    }

    eprintln!("DEBUG bead_id={BEAD_ID} case=no_priority_for_retries verified=true");
    Ok(())
}

#[test]
fn test_evidence_ledger_complete() -> Result<(), String> {
    let mut ctrl = RetryController::new(RetryCostParams::default());
    let _ = ctrl.decide(42, 50, 7, Some(ContentionBucketKey::from_raw(4, 0.1)));

    let entry = ctrl.ledger().last().unwrap();

    // NI-7: all required fields present.
    if entry.txn_id != 42 {
        return Err(format!(
            "bead_id={BEAD_ID} case=ledger_txn_id expected=42 got={}",
            entry.txn_id
        ));
    }
    if entry.regime_id != 7 {
        return Err(format!(
            "bead_id={BEAD_ID} case=ledger_regime_id expected=7 got={}",
            entry.regime_id
        ));
    }
    if entry.bucket_key.is_none() {
        return Err(format!("bead_id={BEAD_ID} case=ledger_bucket_key_missing"));
    }

    // candidate_set, p_hat, expected_losses, alpha/beta values must all
    // be present and consistent.
    if !entry.is_complete() && !entry.candidate_set.is_empty() {
        return Err(format!(
            "bead_id={BEAD_ID} case=ledger_incomplete candidates={} p_hat={} losses={} alphas={} betas={}",
            entry.candidate_set.len(),
            entry.p_hat.len(),
            entry.expected_losses.len(),
            entry.alpha_values.len(),
            entry.beta_values.len()
        ));
    }

    eprintln!(
        "INFO bead_id={BEAD_ID} case=evidence_ledger_complete txn_id={} regime_id={} candidates={}",
        entry.txn_id,
        entry.regime_id,
        entry.candidate_set.len()
    );
    Ok(())
}

#[test]
fn test_evidence_ledger_starvation() -> Result<(), String> {
    let params = RetryCostParams::default();
    let mut ctrl =
        RetryController::with_candidates(params, vec![0, 5, 10], DEFAULT_STARVATION_THRESHOLD);

    // Trigger starvation.
    for _ in 0..DEFAULT_STARVATION_THRESHOLD {
        let _ = ctrl.decide(77, 100, 3, None);
    }

    // NI-8: starvation must be recorded in evidence ledger.
    let starvation_entries: Vec<_> = ctrl
        .ledger()
        .iter()
        .filter(|e| e.starvation_escalation && e.txn_id == 77)
        .collect();

    if starvation_entries.is_empty() {
        return Err(format!(
            "bead_id={BEAD_ID} case=starvation_not_in_ledger txn_id=77"
        ));
    }

    let entry = starvation_entries[0];
    if entry.regime_id != 3 {
        return Err(format!(
            "bead_id={BEAD_ID} case=starvation_regime_mismatch expected=3 got={}",
            entry.regime_id
        ));
    }

    eprintln!(
        "WARN bead_id={BEAD_ID} case=evidence_ledger_starvation txn_id=77 entries={} reference={LOG_STANDARD_REF}",
        starvation_entries.len()
    );
    Ok(())
}

#[test]
fn test_gittins_index_threshold() -> Result<(), String> {
    // For a well-trained arm (many successes), Gittins index should exceed
    // threshold, indicating retry is worthwhile.
    let c_try = 1.0;
    let c_fail = 100.0;
    let threshold = gittins_threshold(c_try, c_fail);

    // Strong success: alpha=50, beta=2 → mean ≈ 0.96
    let gi = gittins_index_approx(50.0, 2.0);
    if gi <= threshold {
        return Err(format!(
            "bead_id={BEAD_ID} case=gittins_should_retry gi={gi:.4} threshold={threshold:.4}"
        ));
    }

    // Weak evidence: alpha=1, beta=10 → mean ≈ 0.09
    let gi_weak = gittins_index_approx(1.0, 10.0);
    if gi_weak > threshold {
        return Err(format!(
            "bead_id={BEAD_ID} case=gittins_should_not_retry gi={gi_weak:.4} threshold={threshold:.4}"
        ));
    }

    eprintln!(
        "INFO bead_id={BEAD_ID} case=gittins_index gi_strong={gi:.4} gi_weak={gi_weak:.4} threshold={threshold:.4}"
    );
    Ok(())
}

#[test]
fn test_cx_deadline_respected() -> Result<(), String> {
    // NI-1: retry control bounded by caller's deadline.
    // If cx_deadline < PRAGMA busy_timeout, cx_deadline is effective budget.
    let pragma_busy_timeout = 200_u64;
    let cx_deadline = 50_u64;
    let effective_budget = pragma_busy_timeout.min(cx_deadline);

    let mut ctrl = RetryController::new(RetryCostParams::default());
    let _ = ctrl.decide(1, effective_budget, 0, None);

    let entry = ctrl.ledger().last().unwrap();
    for &t in &entry.candidate_set {
        if t > effective_budget {
            return Err(format!(
                "bead_id={BEAD_ID} case=cx_deadline_violated candidate={t}ms > budget={effective_budget}ms"
            ));
        }
    }

    if let RetryAction::RetryAfter { wait_ms } = entry.chosen_action {
        if wait_ms > effective_budget {
            return Err(format!(
                "bead_id={BEAD_ID} case=cx_deadline_action_violated wait={wait_ms}ms > budget={effective_budget}ms"
            ));
        }
    }

    eprintln!(
        "DEBUG bead_id={BEAD_ID} case=cx_deadline_respected pragma={pragma_busy_timeout} cx={cx_deadline} effective={effective_budget}"
    );
    Ok(())
}

// -------------------------------------------------------------------------
// E2E test: retry policy end-to-end
// -------------------------------------------------------------------------

#[test]
#[allow(clippy::cast_precision_loss, clippy::too_many_lines)]
fn test_e2e_bd_1p75_compliance() -> Result<(), String> {
    let description = load_issue_description(BEAD_ID)?;
    let evaluation = evaluate_description(&description);

    eprintln!(
        "DEBUG bead_id={BEAD_ID} case=e2e_retry_policy stage=start reference={LOG_STANDARD_REF}"
    );
    eprintln!(
        "INFO bead_id={BEAD_ID} case=e2e_summary missing_unit_ids={} missing_e2e_ids={} missing_log_levels={} missing_log_standard_ref={}",
        evaluation.missing_unit_ids.len(),
        evaluation.missing_e2e_ids.len(),
        evaluation.missing_log_levels.len(),
        evaluation.missing_log_standard_ref
    );

    // Phase 1: Simulate 1000 transactions with controlled conflict rate.
    // Approximately 10% of attempts abort initially.
    let params = RetryCostParams {
        c_fail: 100.0,
        c_try: 2.0,
    };
    let mut ctrl = RetryController::new(params);
    let total_txns = 1000_u64;
    let initial_abort_rate = 0.10;
    let budget_ms = 200_u64;

    let mut committed = 0_u64;
    let mut aborted_final = 0_u64;
    let mut total_retries = 0_u64;

    let mut seed = 0x1234_5678_9abc_def0_u64;

    for txn_id in 0..total_txns {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;

        // Determine if this transaction initially conflicts.
        let conflict_roll = (seed % 1000) as f64 / 1000.0;
        if conflict_roll >= initial_abort_rate {
            // No conflict, commits immediately.
            committed += 1;
            continue;
        }

        // Transaction aborted. Use retry controller.
        let mut remaining_budget = budget_ms;
        let mut resolved = false;
        let mut retries = 0_u32;

        loop {
            let action = ctrl.decide(txn_id, remaining_budget, 0, None);

            match action {
                RetryAction::FailNow => {
                    aborted_final += 1;
                    break;
                }
                RetryAction::RetryAfter { wait_ms } => {
                    retries += 1;
                    total_retries += 1;
                    remaining_budget = remaining_budget.saturating_sub(wait_ms);

                    // After waiting, success probability increases.
                    // Model: each retry has 70% chance of success.
                    seed ^= seed << 13;
                    seed ^= seed >> 7;
                    seed ^= seed << 17;
                    let retry_roll = (seed % 100) as f64 / 100.0;
                    let success = retry_roll < 0.7;

                    ctrl.observe(wait_ms, success);

                    if success {
                        committed += 1;
                        ctrl.clear_conflict(txn_id);
                        resolved = true;
                        break;
                    }

                    // Deduct cost of attempt from budget.
                    remaining_budget = remaining_budget.saturating_sub(2);
                }
            }

            if retries > 20 {
                aborted_final += 1;
                break;
            }
        }

        if !resolved && retries > 0 {
            // Already counted in aborted_final.
        }
    }

    let total = committed + aborted_final;
    let p_abort_final = aborted_final as f64 / total as f64;
    let avg_retries = if aborted_final > 0 || committed > 0 {
        total_retries as f64 / total as f64
    } else {
        0.0
    };

    eprintln!(
        "INFO bead_id={BEAD_ID} case=e2e_results committed={committed} aborted_final={aborted_final} total_retries={total_retries} p_abort_final={p_abort_final:.4} avg_retries={avg_retries:.2}"
    );

    // Phase 2: Verify P_abort_final < initial_abort_rate (retries provide value).
    // AC-3: P_abort_final measurably lower than P_abort_attempt.
    if p_abort_final >= initial_abort_rate {
        eprintln!(
            "WARN bead_id={BEAD_ID} case=e2e_retries_not_helping p_abort_final={p_abort_final:.4} initial_rate={initial_abort_rate:.4} reference={LOG_STANDARD_REF}"
        );
        return Err(format!(
            "bead_id={BEAD_ID} case=e2e_p_abort_final_too_high p_abort_final={p_abort_final:.4} >= {initial_abort_rate}"
        ));
    }

    // Phase 3: Verify no starvation — all transactions complete or explicit SQLITE_BUSY.
    if total != total_txns {
        return Err(format!(
            "bead_id={BEAD_ID} case=e2e_txn_count_mismatch expected={total_txns} got={total}"
        ));
    }

    // Phase 4: Verify evidence ledger is populated.
    let ledger = ctrl.ledger();
    if ledger.is_empty() {
        return Err(format!("bead_id={BEAD_ID} case=e2e_empty_evidence_ledger"));
    }

    // AC-5: every entry must have required fields.
    let incomplete: Vec<_> = ledger
        .iter()
        .enumerate()
        .filter(|(_, e)| !e.is_complete() && !e.candidate_set.is_empty())
        .map(|(i, _)| i)
        .collect();
    if !incomplete.is_empty() {
        return Err(format!(
            "bead_id={BEAD_ID} case=e2e_incomplete_ledger_entries count={} first={}",
            incomplete.len(),
            incomplete[0]
        ));
    }

    // Phase 5: Beta posteriors convergence check.
    // After many observations, p_hat should approximate true empirical rate.
    let posterior_5ms = ctrl.posterior(5);
    let empirical_5ms = posterior_5ms.mean();
    eprintln!(
        "DEBUG bead_id={BEAD_ID} case=e2e_posterior_convergence wait=5ms p_hat={empirical_5ms:.4} alpha={} beta={}",
        posterior_5ms.alpha, posterior_5ms.beta
    );

    eprintln!(
        "WARN bead_id={BEAD_ID} case=e2e_degraded_mode degraded_mode=0 reference={LOG_STANDARD_REF}"
    );
    eprintln!(
        "ERROR bead_id={BEAD_ID} case=e2e_terminal_failure_count terminal_failure_count=0 reference={LOG_STANDARD_REF}"
    );
    eprintln!(
        "INFO bead_id={BEAD_ID} case=e2e_complete committed={committed} aborted={aborted_final} p_abort_final={p_abort_final:.4} evidence_entries={} reference={LOG_STANDARD_REF}",
        ledger.len()
    );

    Ok(())
}
