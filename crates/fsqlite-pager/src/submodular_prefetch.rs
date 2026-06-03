//! Submodular greedy prefetch selection (IMPL-8 / AG-O3 + AAC-P4).
//!
//! Given a budget `B` of pages we can prefetch (bounded by memory/bandwidth)
//! and a candidate set `C` with individual hit-probability estimates, this
//! module selects the subset `S ⊆ C`, `|S| ≤ B`, that maximizes expected
//! cache improvement under a submodular objective.
//!
//! # Mathematical foundation
//!
//! Let `f : 2^C → R⁺` be the expected cache improvement from prefetching a
//! subset `S`. `f` is submodular if
//!
//! ```text
//!   f(S ∪ {p}) - f(S) ≥ f(T ∪ {p}) - f(T)   ∀ S ⊆ T ⊆ C, p ∉ T
//! ```
//!
//! i.e. diminishing returns: adding `p` to a larger selection never yields a
//! larger marginal gain than adding `p` to a smaller one. Under submodularity
//! plus monotonicity (and non-negativity), Nemhauser–Wolsey–Fisher (1978)
//! proved that the **greedy algorithm** — which repeatedly picks the element
//! with maximum marginal gain — achieves
//!
//! ```text
//!   f(S_greedy) ≥ (1 - 1/e) · f(S_opt)  ≈  0.6321 · f(S_opt).
//! ```
//!
//! This `(1 - 1/e)` bound is provably tight for the general monotone
//! submodular maximization problem under a cardinality constraint, so no
//! polynomial-time heuristic can do strictly better without extra structure.
//!
//! # Objective used here
//!
//! ```text
//!   f(S) = Σ_{p ∈ S} hit_prob(p)   -   λ · Σ_{p,q ∈ S, p ≠ q, group(p)=group(q)} hit_prob(p)·hit_prob(q)
//! ```
//!
//! The linear term (`Σ hit_prob`) is trivially submodular (in fact modular).
//! The negative pairwise coverage penalty — paid when two selected pages share
//! an L1 cache-line group (e.g. adjacent pages that would contend for the same
//! prefetch bandwidth) — makes `f` **strictly submodular**, because each newly
//! added page pays the penalty against every previously selected page in the
//! same group. For `λ ≥ 0` and `hit_prob ∈ [0, 1]`, `f` remains monotone as
//! long as `hit_prob(p) ≥ λ · Σ_{q ∈ S, group(q)=group(p)} hit_prob(q)` — in
//! practice we keep `λ` small (e.g. 0.25) so the classic Nemhauser bound
//! applies.
//!
//! # Integration
//!
//! `SimpleTransaction::prefetch_page_hints_greedy` in `pager.rs` calls
//! [`greedy_select`] to pick the page set, then issues a prefetch hint for
//! each selected page. Existing callers of `prefetch_page_hint` are
//! unaffected.

use fsqlite_types::PageNumber;

/// A prefetch candidate: a single page the caller is considering issuing a
/// hardware prefetch hint for, together with the inputs the submodular
/// objective needs.
///
/// - `hit_prob ∈ [0, 1]`: the caller's estimate that this page will be
///   touched soon enough to benefit from a prefetch. Values outside `[0, 1]`
///   are clamped before use.
/// - `cacheline_group`: opaque group identifier. Pages sharing a group pay a
///   pairwise coverage penalty so greedy avoids piling selections onto the
///   same cache line / bandwidth slot. Use `0` to opt out (all pages share a
///   group and a uniform penalty is applied).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Candidate {
    pub page: PageNumber,
    pub hit_prob: f64,
    pub cacheline_group: u16,
}

impl Candidate {
    #[must_use]
    #[inline]
    pub fn new(page: PageNumber, hit_prob: f64, cacheline_group: u16) -> Self {
        Self {
            page,
            hit_prob: clamp01(hit_prob),
            cacheline_group,
        }
    }
}

#[inline]
fn clamp01(x: f64) -> f64 {
    if x.is_nan() { 0.0 } else { x.clamp(0.0, 1.0) }
}

/// Expected objective value `f(S)` for a fully-realised selection `selected`.
///
/// The linear benefit `Σ hit_prob(p)` minus `λ · Σ_{p<q, same group} p·q`
/// pairwise coverage penalty, where `λ = penalty`. `penalty ≤ 0` is treated
/// as `0` (penalty-free / linear regime).
#[must_use]
pub fn objective(selected: &[Candidate], penalty: f64) -> f64 {
    let penalty = penalty.max(0.0);
    let mut linear = 0.0;
    for c in selected {
        linear += c.hit_prob;
    }
    if penalty == 0.0 || selected.len() < 2 {
        return linear;
    }
    let mut pair = 0.0;
    for i in 0..selected.len() {
        for j in (i + 1)..selected.len() {
            if selected[i].cacheline_group == selected[j].cacheline_group {
                pair = selected[i].hit_prob.mul_add(selected[j].hit_prob, pair);
            }
        }
    }
    penalty.mul_add(-pair, linear)
}

/// Marginal gain `f(selected ∪ {candidate}) - f(selected)` for the objective
/// defined in the module docs.
///
/// With `penalty = 0`, this is exactly `candidate.hit_prob` (the linear
/// regime). With `penalty > 0`, it subtracts the pairwise coverage cost
/// incurred by adding `candidate` against every already-selected element in
/// the same `cacheline_group`.
#[must_use]
pub fn expected_gain(selected: &[Candidate], candidate: &Candidate, penalty: f64) -> f64 {
    let penalty = penalty.max(0.0);
    let mut gain = candidate.hit_prob;
    if penalty == 0.0 {
        return gain;
    }
    for s in selected {
        if s.cacheline_group == candidate.cacheline_group {
            gain = (penalty * s.hit_prob).mul_add(-candidate.hit_prob, gain);
        }
    }
    gain
}

/// Nemhauser-Wolsey-Fisher greedy: at each step, pick the candidate with
/// maximum marginal gain. Stops when either `budget` items are selected or no
/// remaining candidate has strictly positive gain.
///
/// For monotone submodular `f`, this returns an `S` with
/// `f(S) ≥ (1 - 1/e) · f(S*)` against the optimum `S*` of size ≤ `budget`.
///
/// Complexity: `O(budget · n)` marginal-gain evaluations, each `O(|S|)`, so
/// `O(budget² · n)` in total. `budget` is typically tiny (≤ 16 for prefetch
/// windows) so this is effectively linear in `n`.
#[must_use]
pub fn greedy_select(candidates: &[Candidate], budget: usize, penalty: f64) -> Vec<PageNumber> {
    if budget == 0 || candidates.is_empty() {
        return Vec::new();
    }
    let penalty = penalty.max(0.0);
    let effective_budget = budget.min(candidates.len());

    // `picked[i] == true` means `candidates[i]` has been taken.
    let mut picked = vec![false; candidates.len()];
    let mut selected: Vec<Candidate> = Vec::with_capacity(effective_budget);
    let mut out: Vec<PageNumber> = Vec::with_capacity(effective_budget);

    for _ in 0..effective_budget {
        let mut best_idx: Option<usize> = None;
        let mut best_gain = 0.0_f64;

        for (i, cand) in candidates.iter().enumerate() {
            if picked[i] {
                continue;
            }
            let gain = expected_gain(&selected, cand, penalty);
            // Strict `>` keeps behaviour deterministic under ties (first
            // occurrence wins) and avoids picking candidates with zero/
            // negative marginal gain — which would violate monotonicity of
            // the running objective for the callers relying on "only
            // prefetch if it helps".
            if gain > best_gain {
                best_gain = gain;
                best_idx = Some(i);
            }
        }

        match best_idx {
            Some(i) => {
                picked[i] = true;
                selected.push(candidates[i]);
                out.push(candidates[i].page);
            }
            None => break,
        }
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pn(n: u32) -> PageNumber {
        PageNumber::new(n).expect("non-zero page number")
    }

    fn c(page: u32, hit_prob: f64, group: u16) -> Candidate {
        Candidate::new(pn(page), hit_prob, group)
    }

    #[test]
    fn budget_zero_returns_empty() {
        let cands = vec![c(1, 0.9, 0), c(2, 0.8, 0)];
        assert!(greedy_select(&cands, 0, 0.0).is_empty());
    }

    #[test]
    fn empty_candidates_returns_empty() {
        let cands: Vec<Candidate> = Vec::new();
        assert!(greedy_select(&cands, 4, 0.25).is_empty());
    }

    #[test]
    fn budget_ge_len_returns_all_sorted_by_gain() {
        let cands = vec![c(1, 0.1, 0), c(2, 0.9, 1), c(3, 0.5, 2)];
        let picked = greedy_select(&cands, 10, 0.0);
        assert_eq!(picked.len(), 3);
        // With penalty = 0 and distinct groups, order = descending hit_prob.
        assert_eq!(picked, vec![pn(2), pn(3), pn(1)]);
    }

    #[test]
    fn penalty_zero_equals_top_k_by_hit_prob() {
        // No cacheline conflicts, penalty = 0 → greedy must equal top-k by
        // hit_prob (property 4 in the IMPL-8 spec).
        let cands = vec![
            c(10, 0.10, 0),
            c(11, 0.95, 1),
            c(12, 0.55, 2),
            c(13, 0.30, 3),
            c(14, 0.80, 4),
        ];
        let picked = greedy_select(&cands, 3, 0.0);
        assert_eq!(picked, vec![pn(11), pn(14), pn(12)]);
    }

    #[test]
    fn greedy_avoids_same_cacheline_group_under_penalty() {
        // Three pages on group 0 (each 0.9) and one page on group 1 (0.7).
        // With budget=2 and a strong penalty, the second pick should prefer
        // the group-1 page even though its raw hit_prob is lower, because
        // adding another group-0 page pays a heavy coverage penalty.
        let cands = vec![c(1, 0.9, 0), c(2, 0.9, 0), c(3, 0.9, 0), c(4, 0.7, 1)];
        let penalty = 1.0; // heavy
        let picked = greedy_select(&cands, 2, penalty);
        assert_eq!(picked.len(), 2);
        // First pick: some group-0 page (tie broken by first-occurrence → pn(1)).
        assert_eq!(picked[0], pn(1));
        // Second pick: group 1 candidate wins because marginal gain for any
        // other group-0 candidate is 0.9 - 1.0·0.9·0.9 = 0.09 < 0.7.
        assert_eq!(picked[1], pn(4));
    }

    #[test]
    fn expected_gain_matches_objective_delta() {
        let cands = vec![c(1, 0.8, 0), c(2, 0.6, 0), c(3, 0.5, 1)];
        let penalty = 0.5;
        let mut running: Vec<Candidate> = Vec::new();
        for cand in &cands {
            let f_before = objective(&running, penalty);
            let marginal = expected_gain(&running, cand, penalty);
            running.push(*cand);
            let f_after = objective(&running, penalty);
            let delta = f_after - f_before;
            assert!(
                (marginal - delta).abs() < 1e-12,
                "expected_gain={marginal} but Δobjective={delta}",
            );
        }
    }

    #[test]
    fn greedy_never_picks_negative_gain() {
        // Construct a pathological case: one strong page on group 0, many
        // weak ones on group 0, and a huge penalty. Greedy must stop rather
        // than pick something with non-positive marginal gain.
        let cands = vec![c(1, 0.9, 0), c(2, 0.1, 0), c(3, 0.1, 0), c(4, 0.1, 0)];
        let picked = greedy_select(&cands, 4, 10.0);
        // First pick is pn(1) (gain 0.9). Adding any other candidate yields
        // marginal = 0.1 - 10 · 0.9 · 0.1 = -0.8 < 0, so greedy stops.
        assert_eq!(picked, vec![pn(1)]);
    }

    /// Brute-force optimum under cardinality constraint for small `n`, used
    /// only in tests to validate the `(1 - 1/e)` approximation bound.
    fn brute_force_optimum(candidates: &[Candidate], budget: usize, penalty: f64) -> f64 {
        let n = candidates.len();
        let budget = budget.min(n);
        let mut best = 0.0_f64;
        // Enumerate all subsets of size ≤ budget.
        for mask in 0u32..(1u32 << n) {
            let popcount = mask.count_ones() as usize;
            if popcount > budget {
                continue;
            }
            let mut sub: Vec<Candidate> = Vec::with_capacity(popcount);
            for (i, cand) in candidates.iter().enumerate() {
                if (mask >> i) & 1 == 1 {
                    sub.push(*cand);
                }
            }
            let v = objective(&sub, penalty);
            if v > best {
                best = v;
            }
        }
        best
    }

    #[test]
    fn greedy_within_one_minus_one_over_e_of_optimum() {
        // Property 3 from IMPL-8: on synthetic traces with known optimum,
        // greedy ≥ (1 - 1/e) · optimum. Exercise several seeded traces.
        let bound = 1.0 - (-1.0_f64).exp(); // 1 - 1/e ≈ 0.6321
        let traces: Vec<(Vec<Candidate>, usize, f64)> = vec![
            (
                vec![
                    c(1, 0.9, 0),
                    c(2, 0.8, 0),
                    c(3, 0.7, 1),
                    c(4, 0.6, 1),
                    c(5, 0.5, 2),
                    c(6, 0.4, 2),
                    c(7, 0.3, 3),
                    c(8, 0.2, 3),
                ],
                3,
                0.5,
            ),
            (
                vec![
                    c(1, 0.95, 0),
                    c(2, 0.94, 0),
                    c(3, 0.93, 0),
                    c(4, 0.50, 1),
                    c(5, 0.49, 2),
                    c(6, 0.48, 3),
                ],
                4,
                1.0, // heavy penalty: optimum spreads across groups
            ),
            (
                vec![
                    c(1, 0.50, 0),
                    c(2, 0.50, 0),
                    c(3, 0.50, 0),
                    c(4, 0.50, 0),
                    c(5, 0.50, 0),
                ],
                3,
                0.25,
            ),
        ];

        for (cands, budget, penalty) in traces {
            let picked_pages = greedy_select(&cands, budget, penalty);
            // Reconstruct picked candidates (in selection order) to score.
            let picked: Vec<Candidate> = picked_pages
                .iter()
                .map(|pn| *cands.iter().find(|c| c.page == *pn).unwrap())
                .collect();
            let greedy_val = objective(&picked, penalty);
            let opt_val = brute_force_optimum(&cands, budget, penalty);
            if opt_val <= 0.0 {
                // Degenerate trace; (1 - 1/e) · 0 = 0, trivially satisfied.
                assert!(greedy_val >= 0.0);
                continue;
            }
            let ratio = greedy_val / opt_val;
            assert!(
                ratio >= bound - 1e-9,
                "greedy={greedy_val} opt={opt_val} ratio={ratio} < (1-1/e)={bound}",
            );
        }
    }

    #[test]
    fn hit_prob_is_clamped_to_unit_interval() {
        let cand = Candidate::new(pn(1), 1.5, 0);
        assert!((cand.hit_prob - 1.0).abs() < 1e-12);
        let cand = Candidate::new(pn(1), -0.3, 0);
        assert!(cand.hit_prob.abs() < 1e-12);
        let cand = Candidate::new(pn(1), f64::NAN, 0);
        assert!(cand.hit_prob.abs() < 1e-12);
    }

    #[test]
    fn negative_penalty_is_treated_as_zero() {
        let cands = vec![c(1, 0.9, 0), c(2, 0.8, 0)];
        let picked_neg = greedy_select(&cands, 2, -5.0);
        let picked_zero = greedy_select(&cands, 2, 0.0);
        assert_eq!(picked_neg, picked_zero);
    }

    #[test]
    fn objective_empty_selection_is_zero() {
        assert!((objective(&[], 0.0)).abs() < 1e-12);
        assert!((objective(&[], 0.5)).abs() < 1e-12);
    }

    #[test]
    fn objective_single_element_equals_hit_prob() {
        let sel = [c(1, 0.73, 0)];
        assert!((objective(&sel, 0.0) - 0.73).abs() < 1e-12);
        assert!((objective(&sel, 1.0) - 0.73).abs() < 1e-12);
    }

    #[test]
    fn greedy_select_all_zero_hit_prob_returns_empty() {
        let cands = vec![c(1, 0.0, 0), c(2, 0.0, 1), c(3, 0.0, 2)];
        assert!(greedy_select(&cands, 3, 0.0).is_empty());
    }

    #[test]
    fn penalty_only_affects_same_group() {
        let selected = [c(1, 0.9, 0)];
        let diff_group = c(2, 0.8, 1);
        let same_group = c(3, 0.8, 0);

        let gain_diff = expected_gain(&selected, &diff_group, 1.0);
        let gain_same = expected_gain(&selected, &same_group, 1.0);

        assert!(
            (gain_diff - 0.8).abs() < 1e-12,
            "different group should have no penalty: {gain_diff}"
        );
        assert!(
            gain_same < gain_diff,
            "same group should pay penalty: {gain_same} vs {gain_diff}"
        );
    }

    #[test]
    fn objective_cross_group_pairs_no_penalty() {
        let sel = [c(1, 0.8, 0), c(2, 0.7, 1)];
        let val = objective(&sel, 1.0);
        assert!(
            (val - 1.5).abs() < 1e-12,
            "different groups: penalty term should be zero, got {val}"
        );
    }

    #[test]
    fn greedy_select_budget_one_picks_highest() {
        let cands = vec![c(3, 0.3, 0), c(7, 0.99, 1), c(5, 0.5, 2)];
        let picked = greedy_select(&cands, 1, 0.25);
        assert_eq!(picked, vec![pn(7)]);
    }

    #[test]
    fn candidate_new_clamps_and_stores_fields() {
        let cand = Candidate::new(pn(42), 0.75, 8);
        assert_eq!(cand.page, pn(42));
        assert!((cand.hit_prob - 0.75).abs() < 1e-12);
        assert_eq!(cand.cacheline_group, 8);
    }

    #[test]
    fn expected_gain_empty_selected_equals_hit_prob() {
        let empty: Vec<Candidate> = Vec::new();
        let cand = c(1, 0.65, 0);
        let gain = expected_gain(&empty, &cand, 0.5);
        assert!(
            (gain - 0.65).abs() < 1e-12,
            "with no prior selection, gain should equal hit_prob, got {gain}"
        );
    }

    #[test]
    fn candidate_debug_copy_partial_eq() {
        let a = c(5, 0.8, 3);
        let b = a;
        assert_eq!(a, b, "Copy must produce equal value");
        let dbg = format!("{a:?}");
        assert!(dbg.contains("Candidate"));
        assert!(dbg.contains("hit_prob"));
        assert!(dbg.contains("cacheline_group"));
    }

    #[test]
    fn clamp01_boundary_values() {
        assert!((clamp01(0.0)).abs() < 1e-15);
        assert!((clamp01(1.0) - 1.0).abs() < 1e-15);
        assert!((clamp01(f64::NEG_INFINITY)).abs() < 1e-15);
        assert!((clamp01(f64::INFINITY) - 1.0).abs() < 1e-15);
        assert!((clamp01(0.5) - 0.5).abs() < 1e-15);
    }

    #[test]
    fn objective_same_group_penalty_exact() {
        let sel = [c(1, 0.8, 0), c(2, 0.6, 0)];
        let val = objective(&sel, 0.25);
        let expected = 0.25_f64.mul_add(-(0.8 * 0.6), 0.8 + 0.6);
        assert!(
            (val - expected).abs() < 1e-12,
            "expected {expected}, got {val}"
        );
    }

    #[test]
    fn greedy_select_tiebreak_first_occurrence() {
        let cands = vec![c(10, 0.5, 1), c(20, 0.5, 2), c(30, 0.5, 3)];
        let picked = greedy_select(&cands, 1, 0.0);
        assert_eq!(
            picked,
            vec![pn(10)],
            "tied candidates: first occurrence wins"
        );
    }
}
